SELECT *, CASE WHEN txn_type IN ('FEE', 'CREDIT') THEN 'Group A' ELSE 'Other' END AS txn_type_group
FROM transactions
WHERE is_flagged;
