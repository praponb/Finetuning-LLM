with kept_keys as (
    -- Set intersection on keys
    select status, customer_id
    from accounts
    intersect all
    select status, customer_id
    from accounts
    where balance <= 54000
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    inner join kept_keys k on s.status = k.status and s.customer_id = k.customer_id
)
select
    customer_id,
    account_type,
    balance,
    opened_date,
    status
from reconciled
order by status desc;
