with enriched as (
    -- Attach accounts attributes
    select
        src.*,
        b.opened_date,
        b.balance,
        b.account_type
    from transactions src
    left join accounts b
        on src.account_id = b.id
    where b.account_type in ('CHECKING', 'SAVINGS')
)
select
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    opened_date,
    account_type
from enriched
order by opened_date desc, txn_type desc;
