SELECT id, merchant, is_flagged, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE amount BETWEEN 1700 AND 3800 ORDER BY 2, is_flagged_flag DESC, id LIMIT 10
