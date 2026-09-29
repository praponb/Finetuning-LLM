select
    case when sqft >= 13090 then 'HIGH' else 'LOW' end as sqft_tier,
    date_trunc('year', opened_date) as year_start,
    round(avg(sqft), 2) as avg_sqft,
    stddev_samp(sqft) as sqft_stddev,
    avg(sqft) filter (where city = 'Chiang Mai') as chiang_mai_avg_sqft,
    percent_rank() over (partition by case when sqft >= 13090 then 'HIGH' else 'LOW' end order by round(avg(sqft), 2) desc) as avg_sqft_percent_rank
from stores
group by case when sqft >= 13090 then 'HIGH' else 'LOW' end, date_trunc('year', opened_date)
