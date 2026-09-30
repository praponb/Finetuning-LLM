WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id >= 18
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'ACTIVE'
              AND p.start_date IS NOT NULL
        )
),
deduped AS (
    -- Keep the latest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY end_date DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 2289.0 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM deduped
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT status, holder_id, premium_tier, premium
    FROM annotated
) src
PIVOT (
    MIN(premium)
    FOR premium_tier IN ('LOW', 'MEDIUM')
)
ORDER BY status, holder_id
