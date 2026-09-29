select i.id, i.quantity, i.last_restock from inventory i where i.product_id in (3, 7, 8, 9) and i.product_id not in (select id from products where launch_date between '2023-03-01' and '2024-01-01');
