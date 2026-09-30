select id, product_id, quantity, percent_rank() over (partition by product_id order by quantity desc) quantity_pct_rank, rank() over (order by quantity) rnk from order_items;
