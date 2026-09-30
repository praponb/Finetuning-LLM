select region, sum(sqft) total_sqft, rank() over (order by sum(sqft) desc) as sqft_rank from stores where month(opened_date) in (6, 7, 12) group by region order by total_sqft desc;
