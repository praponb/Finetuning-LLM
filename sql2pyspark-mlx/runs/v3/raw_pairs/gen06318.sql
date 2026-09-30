select store_id, count(distinct quantity) as unique_products from inventory group by store_id;
