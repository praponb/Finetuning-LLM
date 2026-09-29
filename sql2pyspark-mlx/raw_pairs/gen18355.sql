select city, sum(sqft) as total_sqft, rank() over (order by sum(sqft) desc) as sqft_rank from stores group by city order by total_sqft desc, city limit 20;
