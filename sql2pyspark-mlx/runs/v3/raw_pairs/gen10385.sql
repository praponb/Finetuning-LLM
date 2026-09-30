select
    country,
    sum(rating) as total_rating,
    count(*) as cnt,
    round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
group by country
order by total_rating desc, country
limit 1;
