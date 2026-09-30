SELECT page, COALESCE(duration_sec, 0) AS duration_sec_filled, FLOOR(duration_sec) AS duration_sec_whole FROM web_events WHERE page IS NULL AND duration_sec BETWEEN 228 AND 397
