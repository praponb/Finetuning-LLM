select store_id, count(revenue) as revenue_count, max(revenue) - min(revenue) as revenue_range from store_sales where revenue between 1900 and 3500 group by store_id;
