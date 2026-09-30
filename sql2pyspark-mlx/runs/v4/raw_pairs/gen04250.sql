with supplier_summary as (
    select supplier_id, count(price) as price_count, sum(case when category = 'Electronics' then 1 else 0 end) as electronics_count
    from products
    group by supplier_id
)
select
    s.id,
    s.rating,
    s.country,
    ss.price_count,
    ss.electronics_count
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.price_count > 6;
