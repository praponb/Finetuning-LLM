SELECT id, country, credit_limit * age AS credit_limit_times_age, age - credit_limit AS age_minus_credit_limit FROM customers WHERE credit_limit >= 10600 ORDER BY age_minus_credit_limit DESC;
