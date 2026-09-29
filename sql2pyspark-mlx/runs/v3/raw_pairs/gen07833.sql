select p.category, p.launch_date, i.product_id from products p left join inventory i on p.id = i.product_id where p.launch_date between '2023-12-01' and '2025-05-03' and p.brand in ('Stark', 'Acme')
