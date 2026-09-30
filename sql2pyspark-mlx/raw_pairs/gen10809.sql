select * from (select txn_type, merchant, amount from transactions) pivot (sum(amount) for merchant in ('Grab', 'Shell', 'Lazada')) order by txn_type;
