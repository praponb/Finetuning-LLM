with supplier_summary as (
    select supplier_id, max(price) - min(price) as price_range, sum(case when category = 'Sports' then 1 else 0 end) as sports_count
    from products
    where category != 'Home'
    group by supplier_id
)
select
    s.id,
    s.rating,
    s.country,
    ss.price_range,
    ss.sports_count
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.price_range > 79
order by 4 desc, id
limit 10;
