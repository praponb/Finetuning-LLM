SELECT *, CASE WHEN status IN ('CLOSED', 'OPEN') THEN 'Core' ELSE 'Other' END status_group FROM accounts;
