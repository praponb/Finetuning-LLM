select id, category, array_contains(tags, 'bestseller') as is_bestseller
from products
