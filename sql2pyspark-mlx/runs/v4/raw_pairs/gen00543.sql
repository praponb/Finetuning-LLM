select id, product_id, count(*) over (partition by product_id) as product_id_count from order_items order by id;
