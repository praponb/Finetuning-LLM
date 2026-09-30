select case when s.sqft >= 16084 then 'high' when s.sqft >= 6661 then 'mid' else 'low' end sqft_bucket, round((100 * sum(case when o.channel = 'partner' then 1 else 0 end)) / count(*), 1) pct_partner, round(stddev(o.discount), 2) stddev_discount
from orders o
join stores s on o.store_id = s.id
group by case when s.sqft >= 16084 then 'high' when s.sqft >= 6661 then 'mid' else 'low' end
order by sqft_bucket;
