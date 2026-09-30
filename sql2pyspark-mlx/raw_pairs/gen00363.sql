select
    products.id,
    products.brand,
    suppliers.rating,
    suppliers.supplier_name,
    suppliers.city,
    array_contains(products.tags, 'premium') as is_premium
from products
left join suppliers on suppliers.id = products.supplier_id
where products.brand is not null;
