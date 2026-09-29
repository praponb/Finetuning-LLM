select
    products.brand,
    date_trunc('MONTH', products.launch_date) as launch_month,
    round(stddev(order_items.quantity), 2) as stddev_quantity,
    max(order_items.quantity) as max_quantity
from order_items
left outer join products on order_items.product_id = products.id
full outer join orders on order_items.order_id = orders.id
where order_items.product_id = 6
group by products.brand, date_trunc('MONTH', products.launch_date)
having max(order_items.quantity) >= 6.8
