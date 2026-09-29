select id, merchant, amount from (select *, row_number() over (partition by merchant order by amount, id) as pos from transactions where txn_type in ('DEBIT', 'REFUND')) as r where pos <= 2;
