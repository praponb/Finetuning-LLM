select * from (select txn_type, merchant, amount from transactions where hour(txn_time) between 7 and 11) pivot (avg(amount) for merchant in ('Amazon', 'Lazada', 'Grab')) order by txn_type
