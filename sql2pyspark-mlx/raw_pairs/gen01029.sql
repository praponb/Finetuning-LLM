with enriched as (
    -- Ranking and gap features
    select
        *,
        avg(balance) over (partition by month(opened_date), customer_id order by balance, id rows between 5 preceding and current row) as balance_avg_5,
        avg(balance) over (partition by customer_id order by balance, id rows between 5 preceding and 1 preceding) as balance_avg_5_prior,
        ntile(4) over (partition by status order by balance, id) as balance_bucket
    from accounts
    where year(opened_date) = 2023
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        balance,
        status,
        balance_avg_5_prior,
        balance_bucket,
        min(balance_avg_5) over (partition by status order by balance_bucket, id rows between unbounded preceding and current row) as running_min_balance_avg_5,
        count(balance_avg_5) over (partition by status order by balance_bucket, id rows between unbounded preceding and current row) as running_count_balance_avg_5,
        sum(balance_avg_5) over (partition by status order by balance_bucket, id rows between unbounded preceding and current row) as running_sum_balance_avg_5,
        max(balance_avg_5) over (partition by status order by balance_bucket, id rows between unbounded preceding and current row) as running_max_balance_avg_5
    from enriched
),
matched_keys as (
    -- Set intersection on keys
    select customer_id, id
    from metrics
    intersect
    select customer_id, id
    from accounts
    where account_type = 'LOAN'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join matched_keys k on s.customer_id = k.customer_id and s.id = k.id
),
metrics_accounts as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by running_sum_balance_avg_5 desc nulls first) as running_sum_balance_avg_5_dense_rank,
        cume_dist() over (partition by customer_id order by running_count_balance_avg_5 desc nulls first) as running_count_balance_avg_5_cume_dist,
        min(balance_bucket) over (partition by status) as status_min_balance_bucket,
        count(running_sum_balance_avg_5) over w_grp as customer_id_count_running_sum_balance_avg_5,
        stddev_samp(running_count_balance_avg_5) over w_grp as customer_id_stddev_samp_running_count_balance_avg_5
    from verified
    window w_grp as (partition by customer_id, status)
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics_accounts s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.account_id = 34
        )
)
-- Multi-level aggregation
select
    status,
    count(distinct status) as distinct_status,
    sum(status_min_balance_bucket) as total_status_min_balance_bucket,
    round(sum(running_max_balance_avg_5) / nullif(count(case when status = 'FROZEN' then 1 end), 0), 2) as running_max_balance_avg_5_per_frozen,
    size(collect_set(status)) as num_status,
    round(avg(balance_avg_5_prior), 2) as avg_balance_avg_5_prior,
    count(distinct status) as distinct_status_2
from eligible
group by status
order by running_max_balance_avg_5_per_frozen desc;
