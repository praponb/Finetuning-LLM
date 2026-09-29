select * from (select is_flagged, merchant, amount from transactions)
pivot (avg(amount) for merchant in ('7-Eleven', 'Shell'))
