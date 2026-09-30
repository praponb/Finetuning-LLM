with valid as (
    -- Keep recent, valid rows
    select *, round(monthly_fee, 0) as monthly_fee_rounded
    from subscriptions
    where customer_id is not null
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        status,
        monthly_fee_rounded
    from valid
) src
pivot (
    sum(monthly_fee_rounded) as total,
    count(1) as item_count
    for status in ('EXPIRED', 'CANCELLED', 'ACTIVE')
);
