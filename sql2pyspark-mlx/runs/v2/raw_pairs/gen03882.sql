select * from (select txn_type, merchant, amount from transactions) pivot (count(amount) for merchant in ('Grab', '7-Eleven', 'Starbucks', 'Lazada'));
