select id, account_id, txn_time, merchant from (select *, row_number() over (partition by account_id order by txn_time desc, id) as rnk from transactions) as t where rnk <= 2;
