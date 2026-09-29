select id, brand, explode(tags) as tag from products where price <= 550;
