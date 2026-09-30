select * from (select txn_type, account_id, merchant, amount from transactions) src pivot (max(amount) for merchant in ('Shell', 'Amazon', 'Grab'))
