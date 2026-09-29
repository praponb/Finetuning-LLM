select *, concat_ws(',', brand, category) brand_category_key, substring(category, 1, 3) category_prefix
from products
where product_name is not null and lower(trim(product_name)) = 'running shoes'
