select id, account_id, txn_type, round(amount, 1) as amount_rounded from transactions where account_id in (14, 34) and txn_type <> 'REFUND';
