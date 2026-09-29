select
    city,
    date_format(opened_date, 'yyyy-MM') as opened_month,
    max(opened_date) as latest_opened_date,
    count(distinct sqft) as unique_regions,
    sum(case when region = 'North' then 1 else 0 end) as north_count
from stores
group by 1, 2
having min(sqft) >= 13800
order by latest_opened_date asc, opened_month asc
