select quarter(p.launch_date) as launch_quarter, sum(i.quantity) as total_quantity, avg(i.quantity) as avg_quantity
from inventory i
inner join products p on p.id = i.product_id
group by quarter(p.launch_date)
order by avg_quantity, launch_quarter desc;
