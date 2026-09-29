select year(paid_at) paid_year, sum(amount) total_amount, round((100 * sum(case when method = 'promptpay' then 1 else 0 end)) / count(*), 1) pct_promptpay from payments group by paid_year
