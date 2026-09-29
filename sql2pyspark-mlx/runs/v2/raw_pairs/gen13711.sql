SELECT *, CAST(is_active AS INT) AS is_active_flag, CASE WHEN segment IN ('Consumer', 'SMB') THEN 'Priority' ELSE 'Other' END AS segment_group
FROM customers
WHERE age < 41
ORDER BY city DESC, segment_group;
