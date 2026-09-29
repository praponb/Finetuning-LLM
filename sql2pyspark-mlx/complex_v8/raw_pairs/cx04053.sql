WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label
    FROM sites
    WHERE installed_kw < 373.3
      AND commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality <> 'EST'
        )
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        commissioned_on_label,
        CONCAT_WS('|', commissioned_on_label, tariff) AS commissioned_on_label_tariff_label
    FROM screened
)
SELECT *
FROM (
    SELECT commissioned_on, tariff, installed_kw
    FROM scored
) src
PIVOT (
    AVG(installed_kw)
    FOR tariff IN ('EV', 'TOU')
)
