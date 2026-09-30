with valid as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        substring(txn_type, 1, 4) as txn_type_prefix,
        coalesce(merchant, 'UNKNOWN') as merchant_filled
    from transactions
    where merchant is distinct from 'Lazada'
      and merchant is not null
)
-- Grouped metrics
select
    account_id,
    case when amount >= 4400 then 'LARGE' else 'SMALL' end as amount_tier,
    max(txn_time) as last_seen,
    approx_percentile(amount, 0.9) as amount_p90,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
    grouping_id() as grouping_id
from valid
group by rollup (account_id, case when amount >= 4400 then 'LARGE' else 'SMALL' end)
order by span_minutes desc nulls first
