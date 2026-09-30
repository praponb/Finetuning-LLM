with scored as (
    -- Ranking and gap features
    select
        *,
        lead(amount, 1) over (order by amount, id) next_amount,
        avg(amount) over (order by txn_time, id rows between 6 preceding and current row) amount_avg_6
    from transactions
)
select
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.is_flagged,
    s.next_amount,
    s.amount_avg_6,
    (select min(x.amount) from transactions x where x.account_id = s.account_id) as min_transaction_amount
from scored s
order by amount nulls first
