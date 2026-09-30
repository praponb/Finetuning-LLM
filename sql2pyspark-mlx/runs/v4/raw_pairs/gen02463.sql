SELECT
    id,
    account_id,
    is_flagged,
    merchant
FROM transactions
WHERE merchant NOT IN ('Starbucks', 'Amazon', '7-Eleven') AND txn_time >= '2025-09-01';
