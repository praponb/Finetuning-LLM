WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 6 AND 10
      AND tariff = 'TOU'
)
SELECT id AS ref_id
FROM prepared_tariffs
WHERE rate <= 5.9
EXCEPT
SELECT id AS ref_id
FROM tariff_windows
ORDER BY ref_id
