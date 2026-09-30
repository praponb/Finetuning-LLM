WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        QUARTER(end_date) AS end_date_quarter
    FROM policies
    WHERE start_date BETWEEN DATE '2024-08-27' AND DATE '2025-05-02'
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status <> 'LAPSED'
              AND p.premium BETWEEN 2013.8 AND 4688.7
        )
),
unique_rows AS (
    -- Keep the earliest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        end_date_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY end_date, id) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM unique_rows s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region IN ('South', 'North')
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.holder_risk_score,
        s.holder_region,
        s.holder_segment,
        p.segment AS holder_segment_2,
        p.region AS holder_region_2,
        p.joined_on AS holder_joined_on
    FROM with_details s
    JOIN policyholders p ON p.id = s.holder_id
),
latest_per_key AS (
    -- Keep the latest row per holder_segment, holder_region
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        holder_risk_score,
        holder_region,
        holder_segment,
        holder_segment_2,
        holder_region_2,
        holder_joined_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_segment, holder_region ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM combined_details
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    holder_risk_score,
    holder_region,
    holder_segment,
    holder_segment_2,
    holder_region_2,
    holder_joined_on
FROM latest_per_key
ORDER BY holder_segment, holder_risk_score
