select case when amount >= 1280 then 'high' when amount >= 490 then 'mid' else 'low' end as amount_bucket, round((100 * sum(case when method = 'card' then 1 else 0 end)) / count(*), 1) as pct_card
from payments
where status != 'REFUNDED'
group by case when amount >= 1280 then 'high' when amount >= 490 then 'mid' else 'low' end
order by pct_card desc, amount_bucket desc
limit 1
