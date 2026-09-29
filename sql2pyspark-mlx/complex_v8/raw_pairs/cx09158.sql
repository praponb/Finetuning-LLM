WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        DATE_ADD(start_date, 90) AS start_date_plus_90d,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE status <> 'LAPSED'
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status_ref,
        s.start_date_plus_90d,
        p.segment AS holder_segment
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
stats AS (
    -- Group-level totals
    SELECT
        holder_id,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN holder_segment NOT IN ('VIP', 'SME') THEN 1 ELSE 0 END) AS n_holder_segment_not_in_vip_sm,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM with_details
    GROUP BY holder_id
),
annotated AS (
    -- Label and score rows
    SELECT
        holder_id,
        distinct_ids,
        n_holder_segment_not_in_vip_sm,
        distinct_holder_ids,
        COALESCE(distinct_ids, 0) AS distinct_ids_filled
    FROM stats
    WHERE n_holder_segment_not_in_vip_sm < 7
)
SELECT holder_id, metric_name, metric_value
FROM (
    SELECT holder_id, distinct_holder_ids, distinct_ids_filled
    FROM annotated
) src
UNPIVOT (
    metric_value FOR metric_name IN (distinct_holder_ids, distinct_ids_filled)
) unp
ORDER BY holder_id, metric_name
