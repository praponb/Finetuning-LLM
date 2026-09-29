select account_id, round(avg(amount), 2) as avg_amount, max(amount) as max_amount from transactions where account_id = 28 group by account_id
