select p.id, p.launch_date, s.rating, s.country from products p full outer join suppliers s on s.id = p.supplier_id;
