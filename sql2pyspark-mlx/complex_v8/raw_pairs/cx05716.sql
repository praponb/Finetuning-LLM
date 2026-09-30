WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        CASE WHEN approved_amount >= 18478.3 THEN 'large' WHEN approved_amount >= 4234.9 THEN 'medium' ELSE 'small' END AS approved_amount_tier,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE (policy_id <= 16 OR claim_date >= DATE '2024-08-01')
),
latest AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        approved_amount_tier,
        reported_at_plus_2h,
        status_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY reported_at_plus_2h DESC, id DESC) AS rn
        FROM prepared_claims
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.status IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        claim_type,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        status,
        SUM(CASE WHEN status = 'IN_REVIEW' THEN 1 ELSE 0 END) AS n_status_in_review,
        MAX(reported_at) AS last_reported_at,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        ROUND(100 * COUNT(CASE WHEN claim_type <> 'delay' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_delay,
        MIN(reported_at) AS first_reported_at,
        COUNT(*) AS cnt
    FROM qualified
    GROUP BY claim_type, DATE_FORMAT(claim_date, 'yyyy-MM'), status
),
exploded AS (
    -- One row per claim_type_list_item
    SELECT
        s.claim_type,
        s.claim_date_ym,
        s.status,
        s.n_status_in_review,
        s.last_reported_at,
        s.pct_claim_type_delay,
        s.first_reported_at,
        s.cnt,
        t.claim_type_list_item_pos,
        t.claim_type_list_item
    FROM rollup_base s
    LATERAL VIEW POSEXPLODE(s.claim_type_list) t AS claim_type_list_item_pos, claim_type_list_item
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM exploded
    WHERE cnt > (SELECT MIN(cnt) FROM exploded)
),
claim_type_list_item_stats AS (
    -- Median n_status_in_review per claim_type_list_item
    SELECT claim_type_list_item, PERCENTILE_APPROX(n_status_in_review, 0.5) AS median_n_status_in_review
    FROM with_activity
    GROUP BY claim_type_list_item
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_n_status_in_review,
        ROUND(s.n_status_in_review - g.median_n_status_in_review, 2) AS n_status_in_review_minus_median
    FROM with_activity s
    JOIN claim_type_list_item_stats g ON g.claim_type_list_item = s.claim_type_list_item
    WHERE s.n_status_in_review >= g.median_n_status_in_review
)
-- Final projection
SELECT
    claim_type,
    claim_date_ym,
    status,
    n_status_in_review,
    last_reported_at,
    pct_claim_type_delay,
    first_reported_at,
    cnt,
    claim_type_list_item_pos,
    median_n_status_in_review,
    n_status_in_review_minus_median,
    ROUND(cnt / NULLIF(claim_type_list_item_pos, 0), 3) AS cnt_per_claim_type_list_item_pos
FROM above_average
WHERE median_n_status_in_review >= 4.0
ORDER BY claim_type_list_item_pos DESC, pct_claim_type_delay DESC
