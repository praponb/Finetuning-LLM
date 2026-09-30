select case when rating >= 4.3 then 'high' when rating >= 1.9 then 'mid' else 'low' end as rating_bucket, max(rating) as max_rating
from suppliers
where city = 'Singapore' and city in ('Berlin', 'London', 'Chiang Mai') and upper(trim(supplier_name)) = 'SIAM PARTS CO'
group by 1
