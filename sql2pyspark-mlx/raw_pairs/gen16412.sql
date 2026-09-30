select revenue, units, product_id, coalesce(revenue, 0) as revenue_filled from store_sales where product_id = 8 order by units desc;
