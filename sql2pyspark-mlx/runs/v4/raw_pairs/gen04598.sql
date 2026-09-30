SELECT QUARTER(signup_date) signup_quarter, country, MAX(age) - MIN(age) age_range FROM customers GROUP BY QUARTER(signup_date), country;
