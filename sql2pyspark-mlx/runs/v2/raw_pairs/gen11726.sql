select id, region, sqft, dense_rank() over (partition by region order by sqft) dense_rnk from stores order by region desc
