WITH recent_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CASE WHEN rate >= 5.9 THEN 'large' WHEN rate >= 4.2 THEN 'medium' ELSE 'small' END AS rate_tier
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-18 04:00:00'
      AND tariff <> 'EV'
),
deduped AS (
    -- Keep the latest row per rate_tier
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        rate_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY rate_tier ORDER BY valid_to DESC, id DESC) AS rn
        FROM recent_tariffs
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 3600, 1) AS hours_valid_from_to_valid_to,
        HOUR(valid_from) AS valid_from_hour,
        SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
    FROM deduped
)
SELECT *
FROM (
    SELECT valid_to, valid_from, tariff, rate
    FROM labeled
) src
PIVOT (
    MIN(rate)
    FOR tariff IN ('FLAT', 'EV', 'TOU')
)
ORDER BY valid_to, valid_from
