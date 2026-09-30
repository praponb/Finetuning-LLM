WITH filtered_claims AS (
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
        LAST_DAY(claim_date) AS claim_date_month_end,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE (policy_id >= 23 OR adjuster_id <= 4)
),
summary AS (
    -- Group-level totals
    SELECT
        claim_type,
        adjuster_id,
        DATE_FORMAT(claim_date_month_end, 'yyyy-MM') AS claim_date_month_end_ym,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) FILTER (WHERE claim_type = 'theft') AS n_claim_type_theft
    FROM filtered_claims
    GROUP BY 1, 2, 3
),
scored AS (
    -- Add calculated columns
    SELECT
        claim_type,
        adjuster_id,
        claim_date_month_end_ym,
        status_list,
        COALESCE(n_claim_type_theft, 0) AS n_claim_type_theft_filled,
        CASE WHEN claim_type IN ('collision', 'flood') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM summary
    WHERE adjuster_id IS NOT NULL
),
claim_type_group_stats AS (
    -- Max n_claim_type_theft_filled per claim_type_group
    SELECT claim_type_group, MAX(n_claim_type_theft_filled) AS max_n_claim_type_theft_filled
    FROM scored
    GROUP BY claim_type_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_n_claim_type_theft_filled,
        ROUND(s.n_claim_type_theft_filled - g.max_n_claim_type_theft_filled, 2) AS n_claim_type_theft_filled_minus_max
    FROM scored s
    JOIN claim_type_group_stats g ON g.claim_type_group = s.claim_type_group
    WHERE s.n_claim_type_theft_filled = g.max_n_claim_type_theft_filled
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM outliers s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date
    FROM with_activity s
)
-- Subtotals with CUBE
SELECT
    adjuster_id,
    claim_type,
    n_claim_type_theft_filled,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(max_n_claim_type_theft_filled, 0.5) AS median_max_n_claim_type_theft_filled,
    COUNT(DISTINCT claim_type_group) AS distinct_claim_type_groups
FROM with_stats
GROUP BY CUBE(adjuster_id, claim_type, n_claim_type_theft_filled)
ORDER BY adjuster_id NULLS LAST, claim_type NULLS LAST, n_claim_type_theft_filled NULLS LAST
