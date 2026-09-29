select min(amount) min_amount, round((100 * sum(case when merchant = 'Shell' then 1 else 0 end)) / count(*), 1) pct_shell
from transactions
