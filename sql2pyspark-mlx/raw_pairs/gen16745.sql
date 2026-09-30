SELECT device FROM sessions WHERE device = 'mobile' AND TO_DATE(started_at) >= '2025-07-01' ORDER BY device DESC
