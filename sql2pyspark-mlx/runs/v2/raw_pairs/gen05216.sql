select date_format(opened_date, 'yyyy-MM') as opened_month, sum(case when city = 'London' then 1 else 0 end) as london_count
from stores
group by opened_month
having sum(case when city = 'London' then 1 else 0 end) >= 48;
