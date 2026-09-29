WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(end_date) AS end_date_dayofweek,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        COALESCE(premium, 0.0) AS premium_filled
    FROM scoped
),
combined_details AS (
    -- Join policyholders
    SELECT s.*, p.segment AS holder_segment
    FROM labeled s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, premium, premium_filled
    FROM combined_details
) src
UNPIVOT (
    value FOR metric_name IN (premium AS m_premium, premium_filled AS premium_filled_v)
) unp
ORDER BY id, metric_name
