with kept_keys as (
    -- Set difference on keys
    select account_type
    from accounts
    except all
    select account_type
    from accounts
    where account_type <> 'LOAN'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    join kept_keys k on s.account_type = k.account_type
)
select
    s.customer_id,
    s.balance,
    (select min(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as min_account_balance
from reconciled s;
