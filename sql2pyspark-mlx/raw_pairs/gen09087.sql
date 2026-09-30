with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        service,
        latency_ms,
        attr_key,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as flag_bit1
    from lines
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by service, attr_key order by status_code_high desc nulls first, id asc, attr_key asc) as row_num
        from parsed
    ) x
    where row_num = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        service,
        attr_key,
        attr_value,
        status_code_high,
        row_num,
        avg(status_code_high) over (partition by attr_key, service order by flag_bit1 asc, id asc, attr_key asc rows between 6 preceding and 1 preceding) as status_code_high_avg_6_prior,
        sum(status_code_high) over (order by status_code_high asc, id asc, attr_key asc rows between unbounded preceding and current row) as running_status_code_high,
        lag(flag_bit1, 1) over (partition by attr_key order by latency_ms asc, id asc, attr_key asc) as prev_flag_bit1,
        last_value(attr_key) ignore nulls over (partition by attr_key, service order by row_num asc, id asc, attr_key asc rows between unbounded preceding and current row) as last_known_attr_key,
        avg(latency_ms) over (partition by service, attr_key order by row_num asc, id asc, attr_key asc rows between 6 preceding and current row) as latency_ms_avg_6
    from ranked
)
-- Window metrics over several different partitions
select
    id,
    attr_key,
    row_num,
    last_known_attr_key,
    latency_ms_avg_6,
    row_number() over w_running as event_seq,
    sum(prev_flag_bit1) over (order by status_code_high asc, id asc, attr_key asc rows between unbounded preceding and current row) as running_prev_flag_bit1,
    sum(status_code_high_avg_6_prior) over (partition by attr_key, service) as attr_key_sum_status_code_high_avg_6_prior
from scored
where latency_ms_avg_6 = 876
window w_running as (partition by service order by latency_ms_avg_6 asc, id asc, attr_key asc)
