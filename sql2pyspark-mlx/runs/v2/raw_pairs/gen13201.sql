with supplier_summary as (
    select supplier_id, round(avg(cost), 2) as avg_cost, sum(case when category = 'Electronics' then stock_qty else 0 end) as electronics_stock_qty
    from products
    group by supplier_id
)
select
    s.id,
    s.country,
    ss.avg_cost,
    ss.electronics_stock_qty
from suppliers s
left join supplier_summary ss on s.id = ss.supplier_id
where ss.avg_cost >= 40
order by id desc
