with latest_match as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, latest.transaction_txn_time
    from accounts s
    join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
)
select
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.status,
    case when status rlike '^(FROZEN|OPEN)$' then 'EXTENDED' else 'OTHER_ZONE' end as status_zone,
    (select max(x.balance) from accounts x where x.customer_id = s.customer_id) as max_account_balance
from latest_match s
order by opened_date;
