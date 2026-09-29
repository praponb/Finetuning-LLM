select
    case when sqft >= 16495 then 'PREMIUM' else 'STANDARD' end as sqft_tier,
    bit_and(sqft) as sqft_bit_and,
    bit_and(sqft) as sqft_bit_and_2,
    stddev_samp(sqft) as sqft_stddev,
    round(avg(sqft), 2) as avg_sqft,
    round(avg(sqft), 2) as avg_sqft_2,
    stddev_samp(sqft) as sqft_stddev_2
from stores
group by case when sqft >= 16495 then 'PREMIUM' else 'STANDARD' end
order by sqft_stddev desc
