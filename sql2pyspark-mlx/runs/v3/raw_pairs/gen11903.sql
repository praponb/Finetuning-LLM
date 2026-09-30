select city, sum(rating) as total_rating, round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
group by city
order by total_rating desc;
