select s.city, p.brand from suppliers s left outer join products p on s.id = p.supplier_id where s.city = 'Tokyo'
