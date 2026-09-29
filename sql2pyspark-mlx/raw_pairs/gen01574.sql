select
    d.category,
    d.brand,
    count(*) as num_rows,
    sum(s.quantity) as total_quantity
from inventory s
join products d on s.product_id = d.id
group by grouping sets ((d.category, d.brand), (d.category), ())
order by category nulls last;
