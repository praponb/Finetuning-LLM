select p.category, sum(i.quantity) as total_quantity, count(i.quantity) as quantity_count from inventory i inner join products p on i.product_id = p.id group by p.category having quantity_count >= 3;
