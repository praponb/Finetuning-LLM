select
    id,
    store_id,
    units,
    product_id,
    date_sub(sale_date, 60) sale_date_minus_60d
from store_sales
where (units between 17 and 20 and store_id in (1, 4, 5, 6)) or units <= 37
