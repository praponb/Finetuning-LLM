with kept_keys as (
    select id
    from orders
    except
    select id
    from orders
    where customer_id is not null
),
verified as (
    select s.*
    from orders s
    inner join kept_keys k on s.id = k.id
)
select
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    case when channel rlike '^(web|store)$' then 'PRIORITY' else 'EXTENDED' end as channel_zone
from verified
order by customer_id desc nulls last, order_date
