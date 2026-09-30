WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        ABS(premium - 2824.5) AS premium_distance
    FROM policies
    WHERE holder_id >= 17
),
latest_per_key AS (
    -- Keep the earliest row per product
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        premium_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium_distance, id) AS rn
        FROM base_policys
    ) ranked
    WHERE rn = 1
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_distance,
        p.segment AS holder_segment,
        p.holder_name
    FROM latest_per_key s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        status,
        premium_distance,
        holder_segment,
        holder_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium_distance DESC, id DESC) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date >= DATE '2024-07-18'
              AND c.adjuster_id >= 3
        )
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE premium_distance < (SELECT MAX(premium_distance) FROM filtered)
)
-- Final projection
SELECT
    id,
    holder_id,
    status,
    premium_distance,
    holder_segment
FROM screened
ORDER BY holder_segment DESC NULLS FIRST, holder_id
