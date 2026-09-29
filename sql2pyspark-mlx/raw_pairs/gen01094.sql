select quarter(signup_date) signup_quarter, max(credit_limit) max_credit_limit, count(age) age_count from customers where credit_limit >= 5500 group by quarter(signup_date) order by age_count desc
