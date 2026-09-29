with filtered as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        date_trunc('month', shipped_date) shipped_date_period,
        coalesce(carrier, 'N/A') carrier_clean
    from shipments
    where carrier != 'FedEx'
),
latest as (
    select *
    from (
        select *, row_number() over (partition by status order by shipped_date_period desc nulls last, id) as rank_in_group
        from filtered
    ) x
    where rank_in_group = 1
),
metrics as (
    select
        *,
        first_value(carrier, true) over (order by shipped_date_period, id rows between unbounded preceding and current row) first_known_carrier,
        first_value(carrier, true) over (partition by month(shipped_date), status order by rank_in_group, id rows between unbounded preceding and current row) first_known_carrier_2,
        first_value(carrier, true) over (partition by month(delivered_date), status order by delivered_date, id rows between unbounded preceding and current row) first_known_carrier_3
    from latest
),
ranked as (
    select *
    from metrics
    qualify rank() over (partition by carrier order by delivered_date desc nulls first) = 1
),
metrics_shipments as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        shipped_date_period,
        first_known_carrier,
        first_known_carrier_3,
        row_number() over (partition by status order by shipped_date, id) seq_no,
        last_value(carrier) ignore nulls over (partition by carrier order by shipped_date, id rows between unbounded preceding and current row) last_known_carrier
    from ranked
    where hour(shipped_date_period) between 8 and 12
),
with_windows as (
    select
        id,
        carrier,
        shipped_date_period,
        first_known_carrier_3,
        sum(seq_no) over (partition by year(delivered_date), carrier) delivered_date_year_sum_seq_no,
        sum(seq_no) over (partition by status order by shipped_date_period, id rows between unbounded preceding and current row) running_seq_no
    from metrics_shipments
),
scored as (
    select
        *,
        lead(delivered_date_year_sum_seq_no) over (partition by carrier order by delivered_date_year_sum_seq_no, id) next_delivered_date_year_sum_seq_no,
        lag(delivered_date_year_sum_seq_no) over (partition by carrier order by delivered_date_year_sum_seq_no, id) prev_delivered_date_year_sum_seq_no,
        lag(running_seq_no) over (partition by carrier order by delivered_date_year_sum_seq_no, id) prev_running_seq_no,
        lead(running_seq_no) over (partition by carrier order by delivered_date_year_sum_seq_no, id) next_running_seq_no,
        row_number() over (partition by carrier order by delivered_date_year_sum_seq_no, id) row_seq,
        cume_dist() over (partition by carrier order by delivered_date_year_sum_seq_no desc nulls first) delivered_date_year_sum_seq_no_cume_dist
    from with_windows
)
select
    case when row_seq >= 7 then 'HIGH' else 'LOW' end as row_seq_tier,
    avg(running_seq_no) filter (where carrier = 'DHL') as dhl_avg_running_seq_no,
    sum(delivered_date_year_sum_seq_no_cume_dist) as total_delivered_date_year_sum_seq_no_cume_dist,
    count(*) as event_count
from scored
group by case when row_seq >= 7 then 'HIGH' else 'LOW' end
