select
    id,
    launch_date,
    concat_ws('-', category, product_name) as category_product_name_key,
    concat_ws('-', brand, category) as brand_category_key
from products
where stock_qty between 195 and 344;
