select txn_type, substring(merchant, 1, 2) as merchant_prefix from transactions order by merchant_prefix asc
