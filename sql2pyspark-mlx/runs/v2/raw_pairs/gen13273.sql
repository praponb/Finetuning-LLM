select
    id,
    order_id,
    cost,
    dense_rank() over (partition by order_id order by cost) as dense_rnk
from shipments
where delivered_date >= '2024-02-06';
