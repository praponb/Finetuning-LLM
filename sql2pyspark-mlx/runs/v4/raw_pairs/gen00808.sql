select
    country,
    case when rating >= 4.3 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    count(distinct id) as distinct_ids,
    count_if(city = 'Chiang Mai') as chiang_mai_events
from suppliers
group by country, case when rating >= 4.3 then 'PREMIUM' else 'STANDARD' end
order by chiang_mai_events desc;
