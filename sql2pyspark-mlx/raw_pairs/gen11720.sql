select * from (select txn_type, merchant, amount from transactions) pivot (sum(amount) for merchant in ('7-Eleven', 'Amazon'))
