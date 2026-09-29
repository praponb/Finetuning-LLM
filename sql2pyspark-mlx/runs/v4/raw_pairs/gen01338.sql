select id, merchant, account_id, txn_time, length(merchant) as merchant_length from transactions where hour(txn_time) between 8 and 14 or is_flagged order by merchant_length desc;
