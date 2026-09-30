select id, merchant, txn_time from (select id, merchant, txn_time, rank() over (partition by merchant order by txn_time desc) as rnk from transactions) as ranked where rnk <= 2;
