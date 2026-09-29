SELECT is_flagged, COALESCE(merchant, 'Unknown') merchant_filled FROM transactions WHERE txn_time < '2023-07-17' AND txn_type <> 'CREDIT' AND amount BETWEEN 1400 AND 2500;
