SELECT id AS ref_id, valid_to AS happened_at
FROM tariff_windows
UNION ALL
SELECT id AS ref_id, valid_to AS happened_at
FROM tariff_windows
