select *, ceil(price) price_whole
from products
where supplier_id = 32 and brand = 'Umbrella'
