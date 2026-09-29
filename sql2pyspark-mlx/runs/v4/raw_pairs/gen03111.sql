select date_format(opened_date, 'yyyy-MM') as opened_month, sum(case when region = 'West' then sqft else 0 end) as west_sqft
from stores
where sqft <= 13449
group by opened_month
having west_sqft < 24
order by west_sqft desc;
