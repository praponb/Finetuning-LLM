select region, sqft_count from (select region, count(sqft) as sqft_count from stores group by region) as s where sqft_count >= 3 order by sqft_count desc;
