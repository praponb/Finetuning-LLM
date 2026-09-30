select city, sum(sqft) total_sqft, rank() over (order by sum(sqft) desc) as sqft_rank from stores group by city
