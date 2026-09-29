SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
FROM customers
WHERE is_active = TRUE;
