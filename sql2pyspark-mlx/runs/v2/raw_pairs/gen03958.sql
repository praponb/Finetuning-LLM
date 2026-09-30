select id, status, carrier, delivered_date, cost, lag(cost, 2) over (partition by status, carrier order by delivered_date, id) as prev_cost from shipments where order_id is null order by 3 desc
