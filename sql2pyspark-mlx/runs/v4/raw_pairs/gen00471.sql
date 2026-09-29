SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts DESC) AS dedup_rank
    FROM device_readings
) x
WHERE dedup_rank = 1;
