select * from (select category, supplier_id, brand, price from products) src pivot (sum(price) as amount, count(1) as cnt for brand in ('Initech', 'Acme'));
