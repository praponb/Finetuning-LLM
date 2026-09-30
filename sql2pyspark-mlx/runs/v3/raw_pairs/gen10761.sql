select id, order_id, shipped_date, rank() over (partition by order_id order by shipped_date desc) as rnk from shipments order by order_id;
