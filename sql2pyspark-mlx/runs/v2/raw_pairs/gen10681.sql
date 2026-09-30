select brand, count(case when category = 'Fashion' then 1 end) fashion_count from products where product_name like 'Y%' and supplier_id is not null group by brand
