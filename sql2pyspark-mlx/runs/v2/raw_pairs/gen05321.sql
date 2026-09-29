select
    id,
    supplier_id,
    cost,
    dense_rank() over (partition by supplier_id order by cost) as cost_dense_rank
from products
