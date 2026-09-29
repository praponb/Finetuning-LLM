select case when unit_price >= 440 then 'high' when unit_price >= 90 then 'mid' else 'low' end as unit_price_bucket, count(unit_price) as unit_price_count, count(*) as row_count
from order_items
group by case when unit_price >= 440 then 'high' when unit_price >= 90 then 'mid' else 'low' end
order by unit_price_bucket desc;
