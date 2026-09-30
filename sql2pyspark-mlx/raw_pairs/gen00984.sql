select case when rating >= 4.5 then 'high' when rating >= 1.7 then 'mid' else 'low' end rating_bucket, round(avg(rating), 2) avg_rating
from suppliers
where country in ('TH', 'SG')
group by case when rating >= 4.5 then 'high' when rating >= 1.7 then 'mid' else 'low' end
having avg_rating >= 74
order by rating_bucket desc
