with supplier_summary as (
    select supplier_id, avg(cost) avg_cost, count(stock_qty) stock_qty_count
    from products
    group by supplier_id
)
select s.supplier_name, ss.avg_cost, ss.stock_qty_count
from suppliers s
join supplier_summary ss on s.id = ss.supplier_id
where ss.avg_cost > 630
order by stock_qty_count desc;
