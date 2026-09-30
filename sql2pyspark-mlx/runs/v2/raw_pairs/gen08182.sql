select id, order_id, count(*) over (partition by order_id) as order_id_count from shipments;
