with supplier_summary as (
    select supplier_id, sum(case when category = 'Home' then 1 else 0 end) as home_count, round(stddev(price), 2) as stddev_price
    from products
    group by supplier_id
)
select s.rating, ss.home_count, ss.stddev_price
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.home_count >= 75
