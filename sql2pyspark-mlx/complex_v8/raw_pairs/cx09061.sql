WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE status NOT IN ('IN_REVIEW', 'APPROVED')
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.start_date AS policy_start_date
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        adjuster_level,
        adjuster_name,
        policy_start_date,
        LAG(adjuster_level) OVER w_all_by_adjuster_level AS prev_adjuster_level,
        MIN(adjuster_level) OVER w_adjuster_id AS adjuster_id_min_adjuster_level
    FROM combined_details
    WINDOW w_all_by_adjuster_level AS (ORDER BY adjuster_level DESC, id), w_adjuster_id AS (PARTITION BY adjuster_id)
),
summary AS (
    -- Group-level totals
    SELECT
        prev_adjuster_level,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN LOWER(adjuster_name) LIKE '%ar%' THEN 1 END) / COUNT(*), 1) AS pct_lower_adjuster_name_like_ar,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('theft', 'medical')) AS n_claim_type_not_in_theft_medi
    FROM windowed
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE prev_adjuster_level > (SELECT AVG(prev_adjuster_level) FROM summary)
),
scored AS (
    -- Add calculated columns
    SELECT
        prev_adjuster_level,
        distinct_adjuster_ids,
        distinct_policy_ids,
        pct_lower_adjuster_name_like_ar,
        n_claim_type_not_in_theft_medi,
        LEAST(prev_adjuster_level, distinct_policy_ids) AS least_prev_adjuster_level_distinct_policy_ids,
        ABS(prev_adjuster_level - 3) AS prev_adjuster_level_distance,
        distinct_adjuster_ids - distinct_policy_ids AS distinct_adjuster_ids_minus_distinct_policy_ids
    FROM with_activity
)
-- Final projection
SELECT
    prev_adjuster_level,
    distinct_adjuster_ids,
    distinct_policy_ids,
    n_claim_type_not_in_theft_medi,
    least_prev_adjuster_level_distinct_policy_ids,
    prev_adjuster_level_distance,
    distinct_adjuster_ids_minus_distinct_policy_ids,
    ROUND(distinct_policy_ids / NULLIF(distinct_adjuster_ids, 0), 3) AS distinct_policy_ids_per_distinct_adjuster_ids
FROM scored
ORDER BY distinct_adjuster_ids_minus_distinct_policy_ids DESC, prev_adjuster_level
LIMIT 10
