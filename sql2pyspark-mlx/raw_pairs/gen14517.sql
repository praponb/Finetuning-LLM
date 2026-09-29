with kept_keys as (
    -- Set intersection on keys
    select account_type, status
    from accounts
    intersect
    select account_type, status
    from accounts
    where status is null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    join kept_keys k on s.account_type = k.account_type and s.status = k.status
)
select
    id,
    customer_id,
    opened_date
from kept_rows;
