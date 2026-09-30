select city, count(sqft) sqft_count from stores group by city order by sqft_count, city desc;
