select *
from (
    select
        status,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    max(monthly_fee) as amount,
    count(1) as n
    for plan in ('premium' as premium, 'standard' as standard)
);
