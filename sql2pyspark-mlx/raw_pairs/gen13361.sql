with matched_keys as (
    -- Set intersection on keys
    select id, txn_type
    from transactions
    intersect all
    select id, txn_type
    from transactions
    where merchant in ('Shell', 'Amazon', 'Grab')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from transactions s
    inner join matched_keys k on s.id = k.id and s.txn_type = k.txn_type
),
with_dims as (
    -- Attach accounts attributes
    select
        src.account_id,
        src.txn_type,
        src.merchant,
        src.is_flagged,
        a.balance,
        a.opened_date,
        a.status
    from reconciled src
    inner join accounts a
        on src.account_id = a.id
),
remaining_keys as (
    -- Set intersection on keys
    select merchant
    from with_dims
    intersect
    select merchant
    from transactions
    where account_id in (2, 25)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join remaining_keys k on s.merchant = k.merchant
),
matched_keys_2 as (
    -- Set intersection on keys
    select merchant, account_id
    from kept_rows
    intersect
    select merchant, account_id
    from transactions
    where txn_type in ('REFUND', 'CREDIT')
),
reconciled_transactions as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    inner join matched_keys_2 k on s.merchant = k.merchant and s.account_id = k.account_id
),
matched_keys_3 as (
    -- Set difference on keys
    select txn_type
    from reconciled_transactions
    except
    select txn_type
    from transactions
    where to_date(txn_time) < '2023-04-16'
),
reconciled_transactions_2 as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled_transactions s
    inner join matched_keys_3 k on s.txn_type = k.txn_type
)
select
    s.account_id,
    s.merchant,
    s.opened_date,
    (select max(x.amount) from transactions x where x.account_id = s.account_id) as max_transaction_amount
from reconciled_transactions_2 s;
