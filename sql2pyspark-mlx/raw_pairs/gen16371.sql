select store_id, unique_products from (select store_id, count(distinct units) as unique_products from store_sales where units between 27 and 29 group by store_id) as agg where unique_products < 9;
