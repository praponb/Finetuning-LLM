select
    id,
    category,
    price,
    supplier_id,
    last_value(supplier_id) over (partition by category order by price, id rows between unbounded preceding and unbounded following) as last_supplier_id,
    ntile(10) over (partition by category order by price, id) as bucket
from products
where category not in ('Books', 'Home');
