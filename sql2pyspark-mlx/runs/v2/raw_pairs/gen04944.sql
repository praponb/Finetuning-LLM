select product_id, max(sale_date) latest_sale_date, count(*) store_sale_count, count(revenue) revenue_count from store_sales group by 1 order by 1 desc
