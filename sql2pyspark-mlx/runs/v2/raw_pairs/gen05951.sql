select region, date_format(opened_date, 'yyyy-MM') as opened_month, max(opened_date) as last_opened_date
from stores
where lower(store_name) like '%own%' and city in ('Bangkok', 'Singapore', 'Tokyo')
group by region, opened_month
