select date_trunc('MONTH', paid_at) as paid_month, round((100 * sum(case when method = 'paypal' then 1 else 0 end)) / count(*), 1) as pct_paypal
from payments
group by date_trunc('MONTH', paid_at)
