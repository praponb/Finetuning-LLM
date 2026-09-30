select * from (select txn_type, merchant, amount from transactions where txn_type = 'REFUND') pivot (max(amount) for merchant in ('Lazada', 'Shell', '7-Eleven')) order by txn_type
