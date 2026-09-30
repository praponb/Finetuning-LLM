select id, tags, regexp_replace(product_name, ' ', '_') as product_name_slug from products order by id asc;
