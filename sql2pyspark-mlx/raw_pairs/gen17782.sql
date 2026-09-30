select * from (select txn_type, merchant, amount from transactions) src pivot (sum(amount) for merchant in ('Shell' as shell, 'Grab' as grab));
