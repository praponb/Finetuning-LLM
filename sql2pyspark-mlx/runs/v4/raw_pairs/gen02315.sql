with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from customers s
    where not exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.balance >= 72000
        )
)
select
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    case when segment rlike '^(SMB|Consumer)$' then 'PRIORITY' when segment rlike '^(Corporate)$' then 'CORE' else 'EXTENDED' end as segment_zone
from not_flagged;
