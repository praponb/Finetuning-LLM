SELECT
    is_flagged,
    merchant,
    txn_type,
    CASE WHEN merchant IN ('Starbucks', 'Grab') THEN 'Tier 1' ELSE 'Other' END AS merchant_group,
    CONCAT(merchant, ' ', txn_type) AS merchant_txn_type
FROM transactions;
