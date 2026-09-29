select id, merchant, txn_time, amount, round(avg(amount) over (partition by merchant order by txn_time, id rows between 3 preceding and current row), 2) as amount_moving_avg_4 from transactions
