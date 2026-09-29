WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 15
      AND approved_amount < 25402.3
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM base
    WHERE reported_at >= TIMESTAMP '2025-01-19 13:00:00'
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_ym,
        status,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(*) FILTER (WHERE claim_type <> 'theft') AS n_claim_type_theft,
        MIN(approved_amount) AS min_approved_amount
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', claim_date), DATE_FORMAT(reported_at, 'yyyy-MM'), status
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER w_n_claim_type_theft) AS distinct_status_in_n_claim_type_theft,
        MIN(n_claim_type_theft) OVER (PARTITION BY n_claim_type_theft) AS n_claim_type_theft_min_n_claim_type_theft,
        MAX(n_claim_type_theft) OVER w_n_claim_type_theft AS n_claim_type_theft_max_n_claim_type_theft,
        ROUND(n_claim_type_theft - AVG(n_claim_type_theft) OVER w_n_claim_type_theft, 2) AS n_claim_type_theft_vs_n_claim_type_theft_avg
    FROM per_group
    WINDOW w_n_claim_type_theft AS (PARTITION BY n_claim_type_theft)
)
SELECT claim_date_month, reported_at_ym, metric, reading
FROM (
    SELECT claim_date_month, reported_at_ym, n_claim_type_theft_vs_n_claim_type_theft_avg, min_approved_amount
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric IN (n_claim_type_theft_vs_n_claim_type_theft_avg AS n_claim_type_theft_vs_n_claim_type_theft_avg_v, min_approved_amount AS m_min_approved_amount)
) unp
ORDER BY claim_date_month, reported_at_ym, metric
