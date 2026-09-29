SELECT id, merchant, is_flagged, CAST(is_flagged AS INT) is_flagged_flag FROM transactions WHERE NOT is_flagged ORDER BY 2 ASC
