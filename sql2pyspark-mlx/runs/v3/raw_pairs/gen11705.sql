SELECT
    is_flagged,
    merchant,
    MAX(amount) max_amount,
    COUNT(DISTINCT amount) unique_accounts,
    MAX(amount) - MIN(amount) amount_range
FROM transactions
GROUP BY is_flagged, merchant
HAVING COUNT(DISTINCT amount) < 2;
