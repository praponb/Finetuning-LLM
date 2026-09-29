select * from (select txn_type, merchant, amount from transactions where txn_type = 'REFUND') pivot (sum(amount) for merchant in ('Grab', 'Amazon'))
