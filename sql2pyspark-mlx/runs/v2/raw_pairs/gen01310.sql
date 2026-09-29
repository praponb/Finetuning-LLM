select region, city, max_sqft, row_count from (select region, city, max(sqft) max_sqft, count(*) row_count from stores group by region, city) agg where row_count >= 3;
