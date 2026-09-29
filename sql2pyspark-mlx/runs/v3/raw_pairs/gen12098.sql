select s.city, s.supplier_name, p.category, p.price from suppliers s left outer join products p on p.supplier_id = s.id where p.launch_date <= '2025-10-01' order by p.category desc;
