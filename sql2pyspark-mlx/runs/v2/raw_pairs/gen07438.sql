SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM accounts WHERE opened_date < '2023-09-14' GROUP BY 1 ORDER BY status DESC
