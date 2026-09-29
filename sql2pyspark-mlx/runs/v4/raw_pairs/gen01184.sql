select id, city, regexp_replace(supplier_name, ' ', '_') as supplier_name_slug from suppliers order by supplier_name_slug desc
