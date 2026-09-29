select product_id, count(distinct units) as unique_stores, round(avg(units), 2) as avg_units, max(units) as max_units from store_sales where store_id in (2, 4, 5) group by product_id;
