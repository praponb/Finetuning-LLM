SELECT device, TRIM(device) AS clean_device FROM sessions WHERE browser IS NOT NULL AND TO_DATE(started_at) < '2025-03-07' ORDER BY device DESC
