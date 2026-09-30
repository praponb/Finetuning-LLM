select id, product_id, units - revenue as revenue_per_unit
from store_sales
where units between 12 and 34
