SELECT id, units, store_id, CASE WHEN units >= 31 THEN 'High' WHEN units >= 19 THEN 'Medium' ELSE 'Low' END units_level, COALESCE(revenue, 0) revenue_filled FROM store_sales;
