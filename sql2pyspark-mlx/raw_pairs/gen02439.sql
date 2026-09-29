select p.id, p.price, s.city from products p full outer join suppliers s on p.supplier_id = s.id where s.city in ('New York', 'Bangkok') and s.country != 'UK'
