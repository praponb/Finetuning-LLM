select id, opened_date, sqft, last_day(opened_date) as opened_date_month_end from stores order by id desc, sqft desc limit 10;
