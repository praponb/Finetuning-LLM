select case when rating >= 3.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end rating_bucket, sum(rating) total_rating, round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
where city is not null
group by case when rating >= 3.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end
order by total_rating desc, pct_of_total desc;
