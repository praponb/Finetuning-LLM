select city, max(rating) - min(rating) as rating_range, count(rating) as rating_count
from suppliers
where city <> 'Tokyo'
group by city
order by rating_count;
