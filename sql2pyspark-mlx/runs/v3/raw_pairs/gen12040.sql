select * from (select merchant, txn_type, amount from transactions) pivot (avg(amount) for txn_type in ('REFUND', 'DEBIT')) order by merchant;
