select s.rating, s.city, p.cost, p.brand from suppliers s left outer join products p on s.id = p.supplier_id where s.city not in ('Chiang Mai', 'London', 'Berlin') order by city, brand
