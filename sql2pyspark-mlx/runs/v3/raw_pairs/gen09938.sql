select quarter(opened_date) as opened_quarter, sum(sqft) as total_sqft, dense_rank() over (order by sum(sqft) desc) as rnk from stores where opened_date >= '2024-02-01' group by quarter(opened_date);
