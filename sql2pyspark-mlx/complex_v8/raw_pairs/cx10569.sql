WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('LIFE', 'AUTO') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE (status <> 'CANCELLED' OR holder_id IS NOT NULL)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        product_group,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        CONCAT(product_group, '#', CAST(holder_id AS STRING)) AS product_group_ref
    FROM base_policys
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.product_group,
        s.status_group,
        s.product_group_ref,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        status_group,
        product_group_ref,
        holder_name,
        holder_risk_score,
        holder_region,
        RANK() OVER w_holder_region_holder_id_by_s AS start_date_rank,
        SIZE(COLLECT_SET(holder_region) OVER (PARTITION BY product_group, MONTH(start_date))) AS distinct_holder_region_in_product_group_start_da,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY status_group ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score
    FROM combined_details
    WINDOW w_holder_region_holder_id_by_s AS (PARTITION BY holder_region, holder_id ORDER BY start_date DESC NULLS FIRST, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        start_date,
        product_group_ref,
        holder_name,
        last_known_holder_risk_score,
        ROUND(start_date_rank / NULLIF(holder_risk_score, 0), 3) AS start_date_rank_per_holder_risk_score,
        COALESCE(holder_risk_score, 0.0) AS holder_risk_score_filled
    FROM ranked
)
-- Final projection
SELECT
    id,
    start_date,
    holder_name,
    start_date_rank_per_holder_risk_score,
    holder_risk_score_filled,
    ROUND(holder_risk_score_filled * 0.9, 2) AS holder_risk_score_filled_adj,
    LENGTH(TRIM(product_group_ref)) AS product_group_ref_len
FROM enriched
WHERE start_date IS NOT NULL
ORDER BY id DESC NULLS FIRST
