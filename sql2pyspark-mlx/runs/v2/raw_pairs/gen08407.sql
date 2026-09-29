select quarter(end_date) end_quarter, sum(case when status = 'PAUSED' then monthly_fee else 0 end) paused_monthly_fee
from subscriptions
group by end_quarter
having paused_monthly_fee >= 30;
