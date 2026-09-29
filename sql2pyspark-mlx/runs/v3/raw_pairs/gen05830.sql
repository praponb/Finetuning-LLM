select case when rating >= 3.6 then 'high' when rating >= 2.2 then 'mid' else 'low' end as rating_bucket, sum(rating) as total_rating from suppliers group by 1 having total_rating >= 9
