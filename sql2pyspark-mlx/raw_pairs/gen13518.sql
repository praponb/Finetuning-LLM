with supplier_summary as (
    select supplier_id, sum(case when category = 'Home' then 1 else 0 end) as home_count, count(case when category = 'Books' then 1 end) as books_count
    from products
    group by supplier_id
)
select
    s.id,
    s.city,
    s.rating,
    ss.home_count,
    ss.books_count
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.books_count > 4
order by 1 desc, ss.home_count;
