select c.segment, c.country, round((100 * sum(case when s.plan = 'premium' then 1 else 0 end)) / count(*), 1) as pct_premium
from subscriptions s
inner join customers c on c.id = s.customer_id
group by c.segment, c.country
order by pct_premium desc, segment, country
limit 1
