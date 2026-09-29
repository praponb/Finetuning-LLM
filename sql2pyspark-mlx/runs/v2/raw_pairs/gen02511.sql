select id, merchant, txn_type, count(*) over (partition by merchant, txn_type) merchant_count from transactions order by merchant_count
