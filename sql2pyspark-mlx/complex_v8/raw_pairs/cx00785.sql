WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE premium BETWEEN 1042.8 AND 4670.9
      AND status <> 'ACTIVE'
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM cleaned_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date BETWEEN DATE '2025-06-08' AND DATE '2026-11-25'
              AND p.start_date >= DATE '2024-05-07'
        )
      AND s.end_date BETWEEN DATE '2025-09-27' AND DATE '2026-09-19'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(product)) AS product_clean,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM qualified
),
unique_rows AS (
    -- Keep the latest row per holder_id, product_or_unknown
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        product_clean,
        product_or_unknown,
        days_start_date_to_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product_or_unknown ORDER BY start_date DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.days_start_date_to_end_date,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM unique_rows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', holder_region, holder_segment) AS holder_region_holder_segment_label,
        ADD_MONTHS(start_date, 6) AS start_date_plus_6m
    FROM with_parent_holder
    WHERE start_date IS NOT NULL
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    days_start_date_to_end_date,
    holder_joined_on,
    start_date_plus_6m
FROM scored_2
ORDER BY holder_id NULLS LAST
