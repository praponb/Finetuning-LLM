select s.country, count(p.id) as product_count, max(p.stock_qty) as max_stock_qty
from suppliers s
left outer join products p on s.id = p.supplier_id
group by s.country
having max_stock_qty > 220
order by max_stock_qty desc, country asc
limit 10;
