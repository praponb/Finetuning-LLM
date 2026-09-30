SELECT opened_date, CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'Priority' ELSE 'Other' END AS status_group FROM accounts WHERE status NOT IN ('OPEN', 'CLOSED') ORDER BY status_group;
