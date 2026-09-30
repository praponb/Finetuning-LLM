SELECT id, is_flagged, TRIM(merchant) AS clean_merchant, DATE_TRUNC('week', txn_time) AS txn_time_period FROM transactions
