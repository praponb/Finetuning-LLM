with numbered as (
    select
        id,
        customer_id,
        start_date,
        status,
        dense_rank() over (partition by customer_id order by start_date desc) as row_num
    from subscriptions
)
select
    id,
    customer_id,
    start_date,
    status
from numbered
where row_num <= 2
