select
    status,
    min(paid_at) as first_paid_at,
    max(amount) - min(amount) as amount_range,
    round((100 * sum(case when method = 'card' then 1 else 0 end)) / count(*), 1) as pct_card
from payments
where amount <= 960
group by status;
