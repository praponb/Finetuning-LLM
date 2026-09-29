select id, brand, price, supplier_id, min(price) over (partition by brand) as brand_min_price, row_number() over (partition by supplier_id order by price desc, id) as rn from products
