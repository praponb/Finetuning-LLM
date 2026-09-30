select * from (select merchant, txn_type, amount from transactions) pivot (count(amount) for txn_type in ('DEBIT', 'CREDIT'));
