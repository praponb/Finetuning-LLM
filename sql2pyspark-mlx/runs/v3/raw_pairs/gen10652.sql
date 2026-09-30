select region, count(sqft) as sqft_count from stores where region in ('East', 'North', 'Central') or opened_date >= '2024-04-15' group by region having count(sqft) > 4;
