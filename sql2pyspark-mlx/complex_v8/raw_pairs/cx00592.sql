WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount,
        CASE WHEN approved_amount >= 16404.6 THEN 'A' WHEN approved_amount >= 1273.7 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE claimed_amount <= 34763.4
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_all_by_reported_at)) / 60, 1) AS minutes_since_prev_reported_at,
        LAG(approved_amount) OVER w_all_by_reported_at AS prev_approved_amount,
        LEAD(approved_amount, 1, 0) OVER (ORDER BY reported_at ASC NULLS LAST, id) AS next_approved_amount,
        ROUND(CUME_DIST() OVER w_all_by_reported_at_ties, 3) AS reported_at_cume_dist,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id
    FROM base
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at ASC NULLS LAST, id), w_all_by_reported_at_ties AS (ORDER BY reported_at ASC NULLS LAST), w_policy_id AS (PARTITION BY policy_id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        approved_amount_tier,
        approved_amount_share_of_policy_id,
        minutes_since_prev_reported_at,
        next_approved_amount,
        reported_at_cume_dist,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        LENGTH(TRIM(approved_amount_tier)) AS approved_amount_tier_len
    FROM windowed
    WHERE policy_id <= 24
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        approved_amount_tier,
        adjuster_id,
        COUNT(DISTINCT approved_amount_tier) AS distinct_approved_amount_tiers,
        MAX(minutes_since_prev_reported_at) AS max_minutes_since_prev_reported_at,
        ROUND(SUM(minutes_since_prev_reported_at), 2) AS total_minutes_since_prev_reported_at,
        ROUND(100 * COUNT(CASE WHEN status = 'IN_REVIEW' THEN 1 END) / COUNT(*), 1) AS pct_status_in_review,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MIN(approved_amount) AS min_approved_amount
    FROM annotated
    WHERE reported_at_cume_dist > 0.1
    GROUP BY 1, 2
    HAVING MAX(approved_amount_share_of_policy_id) > 0.2
),
enriched AS (
    -- Join adjusters
    SELECT
        s.approved_amount_tier,
        s.adjuster_id,
        s.distinct_approved_amount_tiers,
        s.total_minutes_since_prev_reported_at,
        s.min_approved_amount,
        a.team AS adjuster_team
    FROM stats s
    JOIN adjusters a ON a.id = s.adjuster_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(min_approved_amount) OVER (ORDER BY total_minutes_since_prev_reported_at ASC, approved_amount_tier, adjuster_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS min_approved_amount_avg_6_prior,
        ROUND(CUME_DIST() OVER w_all_by_total_minutes_since_p_2, 3) AS total_minutes_since_prev_reported_at_cume_dist
    FROM enriched
    WINDOW w_all_by_total_minutes_since_p_2 AS (ORDER BY total_minutes_since_prev_reported_at ASC)
)
-- Final projection
SELECT
    approved_amount_tier,
    adjuster_id,
    distinct_approved_amount_tiers,
    total_minutes_since_prev_reported_at,
    min_approved_amount,
    adjuster_team,
    min_approved_amount_avg_6_prior,
    SUBSTRING(TRIM(approved_amount_tier), 1, 2) AS approved_amount_tier_prefix
FROM sequenced
ORDER BY approved_amount_tier_prefix DESC, adjuster_team
