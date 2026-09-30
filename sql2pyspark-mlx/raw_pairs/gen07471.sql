select
    account_id,
    round(avg(amount), 2) as avg_amount,
    count_if(txn_type = 'CREDIT') as credit_events,
    max(amount) as max_amount,
    min(txn_time) as first_txn_time,
    approx_percentile(amount, 0.5) as amount_p50
from transactions
group by account_id
order by max_amount desc nulls last;
