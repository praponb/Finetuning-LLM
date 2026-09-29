select
    city,
    sum(sqft) as total_sqft,
    count(*) as cnt,
    sum(sum(sqft)) over (order by city) as cumulative_sqft
from stores
where opened_date between '2024-04-01' and '2024-12-01'
group by city
order by city
limit 1;
