with enriched as (
    select
        *,
        avg(amount) over (partition by merchant order by txn_time, id rows between 5 preceding and current row) as amount_avg_5,
        max(amount) over (partition by account_id, txn_type order by cast(txn_time as bigint) range between 7776000 preceding and current row) as amount_max_90d,
        round(amount / sum(amount) over (partition by merchant), 4) as amount_share
    from transactions
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.amount_avg_5,
    s.amount_max_90d,
    s.amount_share,
    case when merchant rlike '^(Amazon|7-Eleven)$' then 'CORE' else 'PRIORITY' end as merchant_zone,
    (select min(t2.amount) from transactions t2 where t2.account_id = s.account_id) as min_transaction_amount
from enriched s
order by amount desc nulls last;
