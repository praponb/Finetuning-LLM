select date_trunc('MONTH', opened_date) as opened_month, count(distinct sqft) as unique_citys from stores group by date_trunc('MONTH', opened_date)
