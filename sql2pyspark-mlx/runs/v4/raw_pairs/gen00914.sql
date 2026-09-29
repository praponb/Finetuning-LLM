select * from (select is_flagged, txn_type, amount from transactions where merchant = '7-Eleven')
pivot (max(amount) for txn_type in ('DEBIT', 'REFUND', 'CREDIT'))
