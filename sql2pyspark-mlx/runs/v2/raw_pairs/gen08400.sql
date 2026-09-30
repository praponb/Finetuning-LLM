select * from (select txn_type, merchant, amount from transactions)
pivot (count(amount) for merchant in ('Amazon', 'Starbucks'))
order by txn_type
