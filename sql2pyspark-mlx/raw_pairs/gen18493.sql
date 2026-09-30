select *
from (
    select
        customer_id,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    avg(monthly_fee)
    for plan in ('enterprise' as enterprise, 'premium' as premium, 'basic' as basic)
)
