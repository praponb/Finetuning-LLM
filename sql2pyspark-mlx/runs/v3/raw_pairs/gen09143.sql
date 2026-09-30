select id, supplier_id, tags, product_name, round(cost, 1) as cost_rounded from products where price between 680 and 750 order by product_name;
