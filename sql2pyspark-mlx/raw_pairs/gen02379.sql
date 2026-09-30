select id, segment, is_active, age, corr(credit_limit, age) over (partition by city) as corr_credit_limit_age, round(age / sum(age) over (partition by city), 4) as age_share from customers
