select id, category, count(*) over (partition by category) as category_count
from products
where brand <> 'Acme';
