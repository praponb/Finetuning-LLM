SELECT 'web_event' AS source, page AS label
FROM web_events
WHERE page IS NOT NULL
UNION ALL
SELECT 'account' AS source, account_type AS label
FROM accounts
WHERE account_type IS NOT NULL
