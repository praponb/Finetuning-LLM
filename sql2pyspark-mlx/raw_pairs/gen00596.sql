select
    id,
    product_name,
    price,
    cost,
    coalesce(price, 0) as price_filled,
    initcap(trim(brand)) as brand_title
from products
where supplier_id = 29 or size(tags) >= 2;
