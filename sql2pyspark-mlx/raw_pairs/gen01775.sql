with latest as (
    -- Keep one row per key (latest first)
    select
        id,
        account_id,
        txn_type,
        txn_time
    from (
        select *, row_number() over (partition by merchant order by amount desc, id) as rank_in_group
        from transactions
    ) t
    where rank_in_group = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, substring(txn_type, 1, 3) as txn_type_prefix
    from latest
    where txn_time < '2023-11-01'
),
latest_transactions as (
    -- Deduplicate with ROW_NUMBER
    select id, account_id
    from filtered
    qualify row_number() over (partition by account_id order by txn_time desc, id) = 1
),
with_dims as (
    -- Attach accounts attributes
    select
        src.id,
        src.account_id,
        d.account_type
    from latest_transactions src
    left join accounts d
        on src.account_id = d.id
)
-- Grouped metrics
select
    account_type,
    account_id,
    size(collect_set(account_type)) as num_account_types,
    size(collect_set(account_type)) as num_account_types_2,
    size(collect_set(account_type)) as num_account_types_3
from with_dims
group by cube (account_type, account_id);
