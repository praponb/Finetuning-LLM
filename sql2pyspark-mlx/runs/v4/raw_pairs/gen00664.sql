with top_products as (select id, brand, cost, supplier_id, rank() over (partition by brand order by cost) as rn from products) select * from top_products where rn <= 3 order by supplier_id desc;
