select id, customer_id, order_date from orders where discount > 0.14 intersect select id, customer_id, order_date from orders where channel in ('store', 'web', 'mobile') order by customer_id;
