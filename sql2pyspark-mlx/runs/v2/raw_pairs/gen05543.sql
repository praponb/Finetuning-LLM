select case when rating >= 4.2 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket, sum(rating) as total_rating, round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
where city = 'Bangkok'
group by case when rating >= 4.2 then 'high' when rating >= 2.0 then 'mid' else 'low' end
order by pct_of_total
