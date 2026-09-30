select i.last_restock, i.product_id, i.quantity, p.supplier_id from inventory i join products p on p.id = i.product_id order by i.last_restock;
