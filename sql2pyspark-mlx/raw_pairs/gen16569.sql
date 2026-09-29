with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from orders
    qualify row_number() over (partition by status order by amount desc nulls first, id) = 1
)
select
    id,
    customer_id,
    order_date,
    status,
    channel,
    discount
from deduped
order by discount desc, channel
