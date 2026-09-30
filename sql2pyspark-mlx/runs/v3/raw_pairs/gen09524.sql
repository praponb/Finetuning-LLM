select quarter(opened_date) as opened_quarter, round(avg(sqft), 2) as avg_sqft, min(opened_date) as min_opened_date
from stores
where store_name like 'Riv%' and sqft = 12948 and city in ('Berlin', 'Tokyo', 'Chiang Mai')
group by 1
order by avg_sqft desc
