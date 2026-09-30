select *, regexp_replace(product_name, ' ', '') as product_name_slug, length(brand) as brand_length
from products
where brand = 'Initech' and cost <= 220 and brand = 'Globex'
