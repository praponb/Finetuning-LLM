select a.customer_id, a.balance, t.is_flagged, t.amount from accounts a inner join transactions t on t.account_id = a.id where t.txn_type not in ('DEBIT', 'FEE') and a.balance > 42000 order by amount
