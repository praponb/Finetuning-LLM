SELECT is_flagged, DATE_TRUNC('YEAR', txn_time) AS txn_time_period FROM transactions WHERE txn_type = 'REFUND';
