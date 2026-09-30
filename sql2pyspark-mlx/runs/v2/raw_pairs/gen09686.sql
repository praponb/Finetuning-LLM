select
    shipments.id,
    shipments.status,
    shipments.cost,
    shipments.delivered_date,
    orders.shipping_fee,
    round(orders.shipping_fee / orders.amount, 2) as amount_with_shipping
from shipments
right join orders on orders.id = shipments.order_id
where orders.amount <= 1080 and year(shipments.delivered_date) = 2024
order by shipments.id desc, 2 desc;
