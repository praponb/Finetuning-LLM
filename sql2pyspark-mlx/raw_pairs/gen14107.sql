with order_totals as (select order_id, count(cost) as cost_count from shipments group by order_id) select o.status, ot.cost_count from orders o left outer join order_totals ot on o.id = ot.order_id
