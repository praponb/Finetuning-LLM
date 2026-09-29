with supplier_stats as (
    select supplier_id, count(*) as cnt
    from products
    group by supplier_id
)
select
    s.id,
    s.country,
    s.rating,
    ss.cnt
from suppliers s
left outer join supplier_stats ss on s.id = ss.supplier_id
where ss.cnt >= 8;
