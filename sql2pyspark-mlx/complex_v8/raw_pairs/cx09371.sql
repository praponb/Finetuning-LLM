WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claimed_amount <= 29359.4
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.product AS policy_product
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_product), 2) AS approved_amount_vs_adjuster_id_policy_product_av,
        ROW_NUMBER() OVER (PARTITION BY policy_product, adjuster_team ORDER BY reported_at, id) AS reported_at_seq,
        SUM(adjuster_level) OVER (PARTITION BY adjuster_id, policy_product) AS adjuster_id_policy_product_sum_adjuster_level,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_product, policy_id), 2) AS claimed_amount_vs_policy_product_policy_id_avg,
        COUNT(*) OVER (PARTITION BY policy_product, policy_id) AS rows_in_policy_product_policy_id,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_id, policy_product), 0), 4) AS adjuster_level_share_of_adjuster_id_policy_produ,
        RANK() OVER (PARTITION BY policy_product, adjuster_team ORDER BY reported_at) AS reported_at_rank,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY policy_product, adjuster_team ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY policy_product, policy_id)) AS distinct_adjuster_team_in_policy_product_policy_,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_product, policy_id)) AS distinct_adjuster_id_in_policy_product_policy_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_level, adjuster_team ORDER BY approved_amount ASC), 3) AS approved_amount_percent_rank,
        FIRST_VALUE(adjuster_level) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_product ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjuster_level
    FROM enriched
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        adjuster_team,
        adjuster_level,
        adjuster_name,
        policy_product,
        approved_amount_vs_adjuster_id_policy_product_av,
        adjuster_id_policy_product_sum_adjuster_level,
        claimed_amount_vs_policy_product_policy_id_avg,
        rows_in_policy_product_policy_id,
        adjuster_level_share_of_adjuster_id_policy_produ,
        reported_at_rank,
        last_claimed_amount,
        distinct_adjuster_team_in_policy_product_policy_,
        distinct_adjuster_id_in_policy_product_policy_id,
        approved_amount_percent_rank,
        first_known_adjuster_level
    FROM sequenced
    WHERE reported_at_seq <= 5
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    adjuster_team,
    adjuster_level,
    approved_amount_vs_adjuster_id_policy_product_av,
    adjuster_id_policy_product_sum_adjuster_level,
    claimed_amount_vs_policy_product_policy_id_avg,
    rows_in_policy_product_policy_id,
    reported_at_rank,
    last_claimed_amount,
    distinct_adjuster_team_in_policy_product_policy_,
    approved_amount_percent_rank,
    first_known_adjuster_level,
    ROUND(claimed_amount / NULLIF(adjuster_level, 0), 3) AS claimed_amount_per_adjuster_level,
    COALESCE(adjuster_level, 0) AS adjuster_level_filled
FROM first_rows
ORDER BY claimed_amount_vs_policy_product_policy_id_avg, id
LIMIT 5
