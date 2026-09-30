SELECT *, CASE WHEN status IN ('CLOSED', 'OPEN') THEN 'Tier 1' ELSE 'Other' END AS status_group, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
FROM accounts
WHERE opened_date >= '2024-03-01' OR account_type IN ('CHECKING', 'CREDIT', 'LOAN');
