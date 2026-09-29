select id, account_id, amount, row_number() over (partition by account_id order by amount desc, id) as rn from transactions;
