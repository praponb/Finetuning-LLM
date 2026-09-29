SELECT id, device FROM sessions EXCEPT ALL SELECT id, device FROM sessions WHERE HOUR(started_at) BETWEEN 7 AND 15;
