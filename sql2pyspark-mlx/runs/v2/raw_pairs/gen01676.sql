with supplier_summary as (
    select supplier_id, round((100 * sum(case when brand = 'Acme' then 1 else 0 end)) / count(*), 1) as pct_acme
    from products
    where category in ('Electronics', 'Home', 'Books')
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    s.city,
    ss.pct_acme
from suppliers s
inner join supplier_summary ss on s.id = ss.supplier_id
where ss.pct_acme >= 51
order by supplier_name desc, city, id
limit 5;
