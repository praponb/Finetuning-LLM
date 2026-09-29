select s.city, t.avg_stock_qty, t.stock_qty_count
from suppliers s
inner join (select supplier_id, round(avg(stock_qty), 2) as avg_stock_qty, count(stock_qty) as stock_qty_count
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.avg_stock_qty > 30
