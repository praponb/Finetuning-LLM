select id, tags
from products
where category in ('Sports', 'Fashion', 'Grocery') and brand <> 'Umbrella' and brand is not null
order by id desc
limit 5
