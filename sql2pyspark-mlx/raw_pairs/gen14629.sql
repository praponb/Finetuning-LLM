select s.id, s.supplier_name, s.city, t.cost_range from suppliers s inner join (select supplier_id, max(cost) - min(cost) cost_range from products group by supplier_id) t on s.id = t.supplier_id;
