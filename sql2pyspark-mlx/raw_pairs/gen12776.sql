select region, sum(sqft) as total_sqft, min(sqft) as min_sqft from stores where not (region in ('West', 'East') and region in ('West', 'North') and sqft < 12177) group by 1;
