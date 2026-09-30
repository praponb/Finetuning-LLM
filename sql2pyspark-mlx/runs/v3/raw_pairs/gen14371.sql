SELECT account_id, TRIM(merchant) clean_merchant FROM transactions WHERE txn_time >= '2024-10-13' AND amount IS NOT NULL ORDER BY account_id;
