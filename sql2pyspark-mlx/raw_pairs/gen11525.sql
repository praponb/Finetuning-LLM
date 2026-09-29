with base as (
    -- Filter the raw rows and derive helper columns
    select *, case when balance >= 65000 then 'High' when balance >= 32000 then 'Medium' else 'Low' end as balance_band
    from accounts
    where opened_date > to_date('2023-10-04')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        balance_band,
        last_value(status) ignore nulls over (partition by account_type, customer_id order by opened_date, id rows between unbounded preceding and current row) as last_known_status,
        lag(balance, 2) over (partition by customer_id, status order by balance, id) as prev_balance,
        first_value(balance, true) over (partition by account_type order by opened_date, id rows between unbounded preceding and current row) as first_known_balance,
        row_number() over (partition by customer_id, status order by balance, id) as event_seq,
        last_value(status) ignore nulls over (partition by year(opened_date), account_type order by opened_date, id rows between unbounded preceding and current row) as last_known_status_2
    from base
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from with_windows
    qualify rank() over (partition by status order by balance desc) = 1
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time >= s.opened_date
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        opened_date,
        status,
        balance_band,
        last_known_status,
        prev_balance,
        last_known_status_2,
        stddev_samp(prev_balance) over (partition by account_type, status) as account_type_stddev_samp_prev_balance,
        avg(event_seq) over (partition by date_trunc('YEAR', opened_date), account_type order by prev_balance, id rows between 2 preceding and 1 preceding) as event_seq_avg_2_prior,
        row_number() over (partition by account_type, status order by first_known_balance desc, id) as event_seq_2
    from eligible
)
-- Aggregate with conditional metrics
select
    status,
    approx_percentile(prev_balance, array(0.25, 0.5, 0.75)) as prev_balance_quartiles,
    count(distinct id) as distinct_ids,
    max(account_type_stddev_samp_prev_balance) filter (where status = 'OPEN') as open_max_account_type_stddev_samp_prev_balance,
    count(distinct status) as distinct_status,
    sum(event_seq_2) filter (where status = 'OPEN') as open_total_event_seq_2,
    grouping_id() as grouping_level
from scored
group by rollup (status)
order by open_total_event_seq_2 desc nulls first
