select s.city, count(distinct p.stock_qty) as unique_brands from products p left join suppliers s on s.id = p.supplier_id group by s.city having count(distinct p.stock_qty) >= 1;
