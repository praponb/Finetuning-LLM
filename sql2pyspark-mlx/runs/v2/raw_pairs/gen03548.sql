select id, product_id, quantity, store_id from inventory where store_id in (1, 2, 5, 6) and (last_restock between '2024-07-25' and '2024-12-29' or last_restock < '2024-12-09') order by quantity desc
