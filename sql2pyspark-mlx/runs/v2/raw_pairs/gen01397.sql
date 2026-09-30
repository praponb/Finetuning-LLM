select id, account_id, amount, sum(amount) over (partition by account_id) as account_id_sum_amount from transactions where account_id in (8, 13, 26)
