with products_by_supplier as (
    select supplier_id, round(stddev(stock_qty), 2) as stddev_stock_qty, avg(price) as avg_price
    from products
    group by supplier_id
)
select
    s.id,
    s.rating,
    s.city,
    pbs.stddev_stock_qty,
    pbs.avg_price
from suppliers s
left join products_by_supplier pbs on s.id = pbs.supplier_id
where pbs.stddev_stock_qty > 24
order by stddev_stock_qty desc;
