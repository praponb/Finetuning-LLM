select *
from (
    select
        customer_id,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    sum(monthly_fee)
    for plan in ('premium', 'enterprise', 'basic')
);
