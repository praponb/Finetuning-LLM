select id, txn_time, rank() over (order by txn_time desc) as rnk from transactions where to_date(txn_time) >= '2023-03-01' order by rnk desc
