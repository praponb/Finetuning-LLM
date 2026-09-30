select p.brand, p.product_name, p.category, s.rating, s.country from products p left outer join suppliers s on p.supplier_id = s.id order by s.country
