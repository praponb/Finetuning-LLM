select
    city,
    sum(rating) as total_rating,
    count(*) as cnt,
    round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
where country <> 'JP'
group by city
order by pct_of_total desc, city
limit 5
