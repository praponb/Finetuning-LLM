WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 0.9, 2) AS premium_adj
    FROM policies
    WHERE status IS NOT NULL
      AND end_date >= DATE '2025-12-05'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        premium + premium_adj AS premium_plus_premium_adj,
        LOWER(TRIM(status)) AS status_clean
    FROM cleaned
)
SELECT *
FROM (
    SELECT start_date, product, premium_plus_premium_adj
    FROM scored
) src
PIVOT (
    SUM(premium_plus_premium_adj)
    FOR product IN ('HOME', 'TRAVEL')
)
