select
    id,
    status,
    plan,
    customer_id,
    ceil(monthly_fee) as monthly_fee_whole
from subscriptions
where plan = 'premium';
