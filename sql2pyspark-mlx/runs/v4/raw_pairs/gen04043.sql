select year(opened_date) as opened_year, date_trunc('MONTH', opened_date) as opened_month, sum(case when region = 'East' then sqft else 0 end) as east_sqft
from stores
where region = 'South' or region = 'West'
group by year(opened_date), date_trunc('MONTH', opened_date)
having sum(case when region = 'East' then sqft else 0 end) > 68
order by opened_year desc, opened_month
limit 5
