with supplier_summary as (
    select supplier_id, round((100 * sum(case when category = 'Books' then 1 else 0 end)) / count(*), 1) as pct_books, count(stock_qty) as stock_qty_count
    from products
    where product_name is not null
    group by supplier_id
)
select
    s.id,
    s.rating,
    s.city,
    ss.pct_books,
    ss.stock_qty_count
from suppliers s
left outer join supplier_summary ss on s.id = ss.supplier_id
order by s.city desc, pct_books
