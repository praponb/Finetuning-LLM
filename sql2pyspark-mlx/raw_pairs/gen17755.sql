select status, count(*) row_count
from orders
where status != 'SHIPPED'
group by status
order by row_count, status
limit 3;
