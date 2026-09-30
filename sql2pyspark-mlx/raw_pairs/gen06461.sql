select region, count(sqft) as sqft_count, round((100 * sum(case when region = 'North' then 1 else 0 end)) / count(*), 1) as pct_north
from stores
where city in ('Berlin', 'Bangkok', 'Chiang Mai') and region is not null and sqft != 9251
group by region;
