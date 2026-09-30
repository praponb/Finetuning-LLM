select case when s.sqft >= 13422 then 'high' when s.sqft >= 3945 then 'mid' else 'low' end sqft_bucket, max(ss.units) max_units, round(avg(ss.units), 2) avg_units
from store_sales ss
right join stores s on ss.store_id = s.id
right join products p on ss.product_id = p.id
group by case when s.sqft >= 13422 then 'high' when s.sqft >= 3945 then 'mid' else 'low' end
order by 1
limit 50
