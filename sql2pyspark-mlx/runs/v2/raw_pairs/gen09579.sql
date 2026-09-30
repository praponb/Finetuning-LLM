select
    id,
    category,
    cost,
    avg(cost) over (partition by category) as category_avg_cost
from products
where array_contains(tags, 'sale')
