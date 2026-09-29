select quarter(opened_date) opened_quarter, sum(sqft) total_sqft, rank() over (order by sum(sqft) desc) as rnk
from stores
group by quarter(opened_date)
order by opened_quarter;
