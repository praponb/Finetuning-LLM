with enriched as (
    -- Running and moving aggregates
    select
        *,
        round(amount / sum(amount) over (partition by to_date(txn_time)), 4) as amount_share,
        sum(amount) over (partition by account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row) as amount_sum_90d
    from transactions
    where amount <= 1100
)
-- Multi-level aggregation
select
    case when amount_sum_90d >= 19787 then 'PREMIUM' else 'STANDARD' end as amount_sum_90d_tier,
    max(txn_time) as last_seen,
    count(*) filter (where merchant = 'Starbucks') as starbucks_rows,
    max(amount_sum_90d) filter (where merchant = 'Amazon') as amazon_max_amount_sum_90d
from enriched
group by case when amount_sum_90d >= 19787 then 'PREMIUM' else 'STANDARD' end
order by amazon_max_amount_sum_90d desc nulls first;
