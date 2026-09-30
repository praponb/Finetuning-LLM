SELECT id, start_date
FROM policies
UNION ALL
SELECT holder_id AS id, end_date AS start_date
FROM policies
