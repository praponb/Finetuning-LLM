WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST) AS latest_rank
        FROM stores
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    store_name,
    sqft,
    latest_rank,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Mall Central)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS store_name_zone
FROM latest
ORDER BY id DESC NULLS LAST, sqft
