with latest_match as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.name,
        s.email,
        s.city,
        s.is_active,
        s.credit_limit,
        s.age,
        latest.end_date,
        latest.monthly_fee
    from customers s
    left join lateral (
            select c.end_date, c.monthly_fee
            from subscriptions c
            where c.customer_id = s.id
              and c.end_date <= s.signup_date
            order by c.end_date desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    name,
    email,
    city,
    is_active,
    credit_limit,
    age,
    end_date,
    monthly_fee
from latest_match;
