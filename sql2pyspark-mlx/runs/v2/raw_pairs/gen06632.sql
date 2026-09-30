select id, store_id, sale_date, floor(revenue) as revenue_whole from store_sales where revenue > 1500;
