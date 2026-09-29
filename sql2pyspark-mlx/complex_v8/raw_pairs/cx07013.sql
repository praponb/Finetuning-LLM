SELECT id AS qty, valid_from AS happened_at
FROM tariff_windows
UNION ALL
SELECT id, valid_to
FROM tariff_windows
