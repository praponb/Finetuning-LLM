with supplier_stats as (
    select supplier_id, sum(case when category = 'Books' then 1 else 0 end) books_count, count(distinct price) unique_suppliers
    from products
    group by supplier_id
)
select
    s.id,
    s.country,
    s.city,
    ss.books_count,
    ss.unique_suppliers
from suppliers s
left join supplier_stats ss on s.id = ss.supplier_id
where ss.unique_suppliers > 4;
