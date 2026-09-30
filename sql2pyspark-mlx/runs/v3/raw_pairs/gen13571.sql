select status, year(shipped_date) as shipped_year, avg(cost) as avg_cost
from shipments
group by status, shipped_year
having count(cost) >= 3;
