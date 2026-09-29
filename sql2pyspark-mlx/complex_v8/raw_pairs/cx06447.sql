WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
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
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-02'
      AND adjuster_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM filtered_claims s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_type_ref,
        claim_type_or_unknown,
        policy_start_date,
        policy_status,
        adjuster_level,
        adjuster_team,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at DESC, id), 3) AS reported_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount), 3) AS approved_amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY policy_status, adjuster_id ORDER BY adjuster_level ASC NULLS LAST, id) AS adjuster_level_seq,
        LEAD(claimed_amount) OVER (ORDER BY reported_at DESC, id) AS next_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_adjuster_level, 0), 4) AS claimed_amount_share_of_adjuster_level,
        SUM(claimed_amount) OVER w_adjuster_level AS adjuster_level_sum_claimed_amount,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount, id) AS approved_amount_change,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount, id) AS rn,
        NTILE(5) OVER (ORDER BY reported_at DESC, id) AS reported_at_quintile,
        NTILE(5) OVER (PARTITION BY policy_status, adjuster_id ORDER BY adjuster_level ASC NULLS LAST, id) AS adjuster_level_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_status, adjuster_id ORDER BY adjuster_level ASC NULLS LAST, id), 3) AS adjuster_level_percent_rank,
        LAST_VALUE(claimed_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount
    FROM with_parent
    WINDOW w_adjuster_level AS (PARTITION BY adjuster_level)
),
stats AS (
    -- Group-level totals
    SELECT
        adjuster_team,
        ROUND(SUM(reported_at_quintile) / NULLIF(SUM(adjuster_level_percent_rank), 0), 3) AS reported_at_quintile_to_adjuster_level_percent_r,
        SUM(adjuster_level_seq) AS total_adjuster_level_seq,
        ROUND(STDDEV(reported_at_quintile), 2) AS stddev_reported_at_quintile,
        SUM(reported_at_quintile) AS total_reported_at_quintile,
        SUM(adjuster_level) AS total_adjuster_level,
        COUNT(*) AS n_rows
    FROM metrics
    WHERE claim_date IS NOT NULL
    GROUP BY adjuster_team
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        adjuster_team,
        stddev_reported_at_quintile,
        total_adjuster_level,
        n_rows,
        LEAD(n_rows) OVER w_all_by_total_adjuster_level AS next_n_rows,
        MAX(stddev_reported_at_quintile) OVER (PARTITION BY n_rows, adjuster_team) AS n_rows_adjuster_team_max_stddev_reported_at_quin
    FROM stats
    WINDOW w_all_by_total_adjuster_level AS (ORDER BY total_adjuster_level_seq DESC, adjuster_team)
)
-- Final projection
SELECT
    adjuster_team,
    stddev_reported_at_quintile,
    total_adjuster_level,
    n_rows,
    next_n_rows,
    LOWER(REGEXP_REPLACE(TRIM(adjuster_team), ' ', '_')) AS adjuster_team_slug,
    FLOOR(total_adjuster_level / 5) * 5 AS total_adjuster_level_bucket
FROM ranked
ORDER BY total_adjuster_level DESC, next_n_rows NULLS LAST
