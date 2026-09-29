SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM accounts
GROUP BY 1
ORDER BY status
