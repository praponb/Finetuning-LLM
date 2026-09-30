with supplier_stats as (
    select supplier_id, round((100 * sum(case when brand = 'Umbrella' then 1 else 0 end)) / count(*), 1) as pct_umbrella, sort_array(collect_set(category)) as category_list
    from products
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    s.city,
    ss.pct_umbrella,
    ss.category_list
from suppliers s
join supplier_stats ss on s.id = ss.supplier_id
order by supplier_name, id
limit 5;
