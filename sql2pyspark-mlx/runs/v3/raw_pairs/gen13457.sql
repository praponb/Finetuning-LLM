select id, product_id, sale_date, revenue, sum(revenue) over (partition by product_id order by sale_date, id) as running_revenue from store_sales order by revenue desc;
