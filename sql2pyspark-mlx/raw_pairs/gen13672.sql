select * from transactions where not (amount >= 3100 and (txn_type != 'FEE' or merchant is not null))
