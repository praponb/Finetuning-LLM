select s.supplier_name, t.pct_electronics
from suppliers s
inner join (select supplier_id, round((100 * sum(case when category = 'Electronics' then 1 else 0 end)) / count(*), 1) as pct_electronics
    from products
    where price < 630
    group by supplier_id) t on s.id = t.supplier_id
where t.pct_electronics > 75
order by s.supplier_name
