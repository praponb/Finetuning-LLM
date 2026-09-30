select date_trunc('MONTH', p.launch_date) as launch_month, max(i.quantity) as max_quantity
from inventory i
full outer join products p on i.product_id = p.id
full outer join suppliers s on s.id = p.supplier_id
where s.city is not null
group by date_trunc('MONTH', p.launch_date)
