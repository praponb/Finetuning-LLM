select id, order_id, delivered_date, cost, lag(cost, 1, 0) over (partition by order_id order by delivered_date, id) as prev_cost from shipments
