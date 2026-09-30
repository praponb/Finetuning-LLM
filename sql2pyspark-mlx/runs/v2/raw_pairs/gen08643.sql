select * from (select id, store_id, revenue, product_id, rank() over (partition by store_id order by revenue) as rn from store_sales) as r where rn = 1
