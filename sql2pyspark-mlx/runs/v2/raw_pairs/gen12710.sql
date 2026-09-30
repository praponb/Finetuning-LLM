select year(signup_date) signup_year, round((100 * sum(case when city = 'Chiang Mai' then 1 else 0 end)) / count(*), 1) pct_chiang_mai from customers group by signup_year order by signup_year;
