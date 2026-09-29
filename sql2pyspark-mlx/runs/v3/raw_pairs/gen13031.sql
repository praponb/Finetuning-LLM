select
    s.id,
    s.country,
    count(p.id) as product_count,
    sum(case when p.category = 'Sports' then p.cost else 0 end) as sports_cost
from suppliers s
left outer join products p on p.supplier_id = s.id
where p.stock_qty >= 329
group by s.id, s.country
