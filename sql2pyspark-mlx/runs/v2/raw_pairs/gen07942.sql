select * from (select merchant, txn_type, amount from transactions where account_id = 1)
pivot (avg(amount) for txn_type in ('CREDIT', 'DEBIT', 'REFUND'));
