SELECT status, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count FROM payments WHERE status <> 'SUCCESS' GROUP BY 1 ORDER BY success_count DESC
