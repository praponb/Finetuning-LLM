with latest_match as (
    select
        s.id,
        s.account_type,
        latest.amount
    from accounts s
    left join lateral (
            select c.amount
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
latest as (
    select *, row_number() over (partition by account_type order by amount, id) as rn
    from latest_match
    qualify rn = 1
),
ranked as (
    select id, amount
    from (
        select *, row_number() over (partition by account_type order by rn desc, id) as rn_2
        from latest
    ) x
    where rn_2 = 1
),
with_last as (
    select s.*, top1.transaction_txn_type
    from ranked s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    transaction_txn_type,
    ntile(10) over w_part as amount_bucket,
    row_number() over (partition by transaction_txn_type order by amount desc, id) as row_seq,
    ntile(4) over w_part as amount_bucket_2
from with_last
window w_part as (partition by transaction_txn_type order by amount, id);
