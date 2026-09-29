select id, rating, round(rating * 0.85, 2) as adjusted_rating
from suppliers
where supplier_name like 'Eur%'
order by rating, id
limit 50;
