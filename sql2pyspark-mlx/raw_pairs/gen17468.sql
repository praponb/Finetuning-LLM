select p.price, p.cost, i.store_id, round(i.quantity, 1) quantity_rounded from products p left join inventory i on p.id = i.product_id where i.last_restock <= '2024-05-01'
