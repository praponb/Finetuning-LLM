select s.id, count(p.id) as product_count, round(avg(p.price), 2) as avg_price from suppliers s left join products p on s.id = p.supplier_id where p.brand in ('Globex', 'Acme') group by s.id;
