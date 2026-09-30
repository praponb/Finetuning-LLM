select id, country, credit_limit, round(credit_limit / sum(credit_limit) over (partition by country), 3) as credit_limit_share from customers order by country
