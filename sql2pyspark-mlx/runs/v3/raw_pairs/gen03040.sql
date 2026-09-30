select s.country, count(p.id) product_count, min(p.cost) min_cost
from suppliers s
left join products p on s.id = p.supplier_id
group by s.country
having min_cost >= 200
order by s.country
