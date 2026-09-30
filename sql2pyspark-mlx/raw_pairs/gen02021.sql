select
    id,
    customer_id,
    started_at,
    country
from (
    select
        id,
        customer_id,
        started_at,
        country,
        row_number() over (partition by customer_id order by started_at desc, id) as pos
    from sessions
) as ranked
where pos <= 3
