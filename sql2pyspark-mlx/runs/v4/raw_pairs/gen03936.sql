select method, round((100 * sum(case when method = 'paypal' then 1 else 0 end)) / count(*), 1) as pct_paypal from payments group by method order by method;
