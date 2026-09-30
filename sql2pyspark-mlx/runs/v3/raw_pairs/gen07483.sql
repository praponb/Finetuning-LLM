select plan, pct_premium
from (
    select plan, round((100 * sum(case when plan = 'premium' then 1 else 0 end)) / count(*), 1) as pct_premium
    from subscriptions
    where monthly_fee between 80 and 100
    group by plan
) as sub
where pct_premium < 37
order by plan desc, pct_premium desc
