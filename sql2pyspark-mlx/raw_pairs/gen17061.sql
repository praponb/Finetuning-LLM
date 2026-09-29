select id, cost, initcap(trim(brand)) as brand_title from products where price between 510 and 990 and (price <= 1000 or month(launch_date) in (3, 10, 11));
