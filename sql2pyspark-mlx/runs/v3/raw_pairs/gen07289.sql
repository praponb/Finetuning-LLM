select
    id,
    product_id,
    sale_date,
    rank() over (partition by product_id order by sale_date) as sale_date_rank
from store_sales
where revenue between 1100 and 2300;
