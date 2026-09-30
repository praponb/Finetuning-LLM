WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        YEAR(reported_at) AS reported_at_year,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at_year,
        status_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM recent_claims
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        ROUND(reported_at_year - AVG(reported_at_year) OVER w_policy_id_status, 2) AS reported_at_year_vs_policy_id_status_avg,
        NTILE(10) OVER (PARTITION BY reported_at_year ORDER BY approved_amount DESC, id) AS approved_amount_decile,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, status), 0), 4) AS approved_amount_share_of_policy_id_status,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, status)) AS distinct_claim_type_in_policy_id_status,
        ROUND(reported_at_year / NULLIF(SUM(reported_at_year) OVER (PARTITION BY policy_id, status), 0), 4) AS reported_at_year_share_of_policy_id_status,
        NTILE(4) OVER (PARTITION BY status_or_unknown, policy_id ORDER BY approved_amount, id) AS approved_amount_quartile
    FROM latest_per_key
    WINDOW w_policy_id_status AS (PARTITION BY policy_id, status)
),
stats AS (
    -- Group-level totals
    SELECT
        approved_amount_decile,
        approved_amount_quartile,
        distinct_claim_type_in_policy_id_status,
        SUM(approved_amount_decile) AS total_approved_amount_decile,
        SUM(CASE WHEN claim_type IN ('collision', 'fire') THEN approved_amount ELSE 0 END) AS approved_amount_claim_type_in_collision_fire,
        ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount
    FROM metrics
    WHERE reported_at_year_share_of_policy_id_status > 0.1
    GROUP BY approved_amount_decile, approved_amount_quartile, distinct_claim_type_in_policy_id_status
)
SELECT approved_amount_decile, approved_amount_quartile, metric_name, reading
FROM (
    SELECT approved_amount_decile, approved_amount_quartile, total_approved_amount_decile, distinct_claim_type_in_policy_id_status
    FROM stats
) src
UNPIVOT (
    reading FOR metric_name IN (total_approved_amount_decile, distinct_claim_type_in_policy_id_status)
) unp
