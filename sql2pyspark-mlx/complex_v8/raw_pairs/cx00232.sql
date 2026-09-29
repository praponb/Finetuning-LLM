WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM claims
    WHERE adjuster_id >= 2
),
with_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        p.product AS policy_product
    FROM base s
    JOIN policies p ON p.id = s.policy_id AND p.status NOT IN ('EXPIRED', 'LAPSED')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        NTILE(4) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS approved_amount_quartile,
        LAG(approved_amount, 1, 0) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS prev_approved_amount,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS approved_amount_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id), 3) AS approved_amount_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY approved_amount), 3) AS approved_amount_percent_rank,
        SUM(approved_amount) OVER (PARTITION BY status ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior
    FROM with_details
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.approved_amount_quartile,
        s.prev_approved_amount,
        s.approved_amount_tercile,
        s.approved_amount_cume_dist,
        s.approved_amount_percent_rank,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM ranked s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.approved_amount_quartile,
        s.prev_approved_amount,
        s.approved_amount_tercile,
        s.approved_amount_cume_dist,
        s.approved_amount_percent_rank,
        s.adjuster_team,
        s.adjuster_name,
        p.premium AS policy_premium
    FROM with_parent_adjuster s
    LEFT JOIN policies p ON p.id = s.policy_id
),
latest AS (
    -- Keep the latest row per adjuster_id, adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        approved_amount_quartile,
        prev_approved_amount,
        approved_amount_tercile,
        approved_amount_cume_dist,
        approved_amount_percent_rank,
        adjuster_team,
        adjuster_name,
        policy_premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, adjuster_team ORDER BY approved_amount_cume_dist DESC, id DESC) AS recency_rank
        FROM joined_policy
    ) ranked
    WHERE recency_rank = 1
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.approved_amount_quartile,
        s.prev_approved_amount,
        s.approved_amount_cume_dist,
        s.approved_amount_percent_rank,
        s.adjuster_name,
        s.policy_premium,
        a.level AS adjuster_level
    FROM latest s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, adjuster_level, approved_amount_quartile
    FROM joined_adjuster
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (adjuster_level, approved_amount_quartile)
) unp
ORDER BY id, metric_name
