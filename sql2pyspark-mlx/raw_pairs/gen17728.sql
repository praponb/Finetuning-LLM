with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        add_months(opened_date, 6) as opened_date_plus_months,
        coalesce(status, 'DIRECT') as resolved_status
    from accounts
    where balance >= 60000
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        balance,
        opened_date,
        opened_date_plus_months,
        resolved_status,
        last_value(balance, true) over w_running as last_known_balance,
        lead(balance, 2) over (partition by customer_id order by balance, id) as next_balance,
        row_number() over (partition by status order by balance, id) as row_seq,
        sum(balance) over (partition by status, account_type order by opened_date_plus_months, id rows between unbounded preceding and current row) as cumulative_balance,
        percent_rank() over w_recent as balance_percent_rank
    from filtered
    where balance > 68000
    window w_running as (partition by status order by opened_date_plus_months, id rows between unbounded preceding and current row), w_recent as (partition by customer_id, status order by balance)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and hour(c.txn_time) between 7 and 13
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        balance,
        opened_date_plus_months,
        resolved_status,
        row_seq,
        cumulative_balance,
        balance_percent_rank,
        row_number() over (order by balance_percent_rank desc, id) as row_seq_2,
        lead(balance_percent_rank, 2) over (order by cumulative_balance, id) as next_balance_percent_rank
    from kept
)
-- Multi-level aggregation
select
    case when next_balance_percent_rank >= 0.7 then 'HIGH' else 'LOW' end as next_balance_percent_rank_tier,
    date_trunc('week', opened_date_plus_months) as week_start,
    percentile_approx(next_balance_percent_rank, array(0.25, 0.5, 0.75)) as next_balance_percent_rank_quartiles,
    percentile_approx(next_balance_percent_rank, 0.5) as next_balance_percent_rank_p50,
    round(avg(next_balance_percent_rank), 2) as avg_next_balance_percent_rank,
    percentile_approx(balance, 0.5) as balance_p50,
    sum(balance_percent_rank) as total_balance_percent_rank,
    max(balance_percent_rank) as max_balance_percent_rank,
    grouping_id() as grouping_id
from enriched
group by grouping sets ((case when next_balance_percent_rank >= 0.7 then 'HIGH' else 'LOW' end, date_trunc('week', opened_date_plus_months)), (case when next_balance_percent_rank >= 0.7 then 'HIGH' else 'LOW' end));
