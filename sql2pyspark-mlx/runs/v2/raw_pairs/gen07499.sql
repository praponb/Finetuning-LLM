select * from (select status, year(end_date) end_year, round(avg(monthly_fee), 2) avg_monthly_fee from subscriptions group by status, year(end_date)) x where avg_monthly_fee > 41;
