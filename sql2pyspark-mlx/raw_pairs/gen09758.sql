with enriched as (
    -- Ranking and gap features
    select
        id,
        budget,
        lag(budget) over (partition by dept_name order by budget, id) prev_budget,
        lead(budget) over (partition by dept_name order by budget, id) next_budget,
        row_number() over (partition by dept_name order by budget, id) row_seq,
        last_value(dept_name) ignore nulls over (partition by dept_name order by budget, id rows between unbounded preceding and current row) last_known_dept_name
    from departments
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        budget,
        row_seq,
        sum(row_seq) over (order by row_seq, id rows between unbounded preceding and current row) cumulative_row_seq,
        row_number() over (order by row_seq desc, id) event_seq
    from enriched
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.hire_date,
        x.salary
    from with_windows s
    left join lateral (
            select c.hire_date, c.salary
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
matched_keys as (
    -- Set intersection on keys
    select id
    from with_last
    intersect all
    select id
    from departments
    where budget <= 1691000
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last s
    join matched_keys k on s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over w_part salary_dense_rank,
        percent_rank() over (order by event_seq desc) event_seq_percent_rank,
        cume_dist() over (order by cumulative_row_seq nulls last) cumulative_row_seq_cume_dist,
        dense_rank() over w_hist event_seq_dense_rank
    from reconciled
    window w_part as (partition by month(hire_date) order by salary nulls last), w_hist as (order by event_seq desc nulls first)
),
with_windows_departments as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by event_seq_percent_rank nulls first) event_seq_percent_rank_cume_dist,
        dense_rank() over (order by event_seq desc nulls last) event_seq_dense_rank_2,
        dense_rank() over (order by event_seq_dense_rank nulls last) event_seq_dense_rank_dense_rank
    from metrics
)
-- Aggregate with conditional metrics
select
    date_trunc('week', hire_date) as week_start,
    case when event_seq_dense_rank_dense_rank >= 8 then 'HIGH' else 'LOW' end as event_seq_dense_rank_dense_rank_tier,
    approx_percentile(event_seq_dense_rank, array(0.25, 0.5, 0.75)) as event_seq_dense_rank_quartiles,
    count(*) as row_count,
    count(distinct id) as distinct_ids,
    approx_percentile(cumulative_row_seq, array(0.25, 0.5, 0.75)) as cumulative_row_seq_quartiles,
    percentile_approx(event_seq_dense_rank, array(0.25, 0.5, 0.75)) as event_seq_dense_rank_quartiles_2,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles
from with_windows_departments
group by week_start, event_seq_dense_rank_dense_rank_tier
having distinct_ids > 1
order by week_start
