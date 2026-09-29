select * from (select txn_type, merchant, amount from transactions)
pivot (avg(amount) for merchant in ('7-Eleven', 'Amazon', 'Starbucks', 'Grab'))
order by txn_type;
