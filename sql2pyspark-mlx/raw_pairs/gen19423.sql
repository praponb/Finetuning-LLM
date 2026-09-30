select
    id,
    city,
    country,
    credit_limit,
    segment,
    last_value(segment) over (partition by city, country order by credit_limit, id rows between unbounded preceding and unbounded following) as last_segment
from customers
