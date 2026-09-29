SELECT channel
FROM tickets
WHERE ARRAY_CONTAINS(tags, 'refund')
EXCEPT
SELECT priority AS channel
FROM tickets
WHERE csat >= 2
