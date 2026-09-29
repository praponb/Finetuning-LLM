WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        CONCAT_WS('|', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE policy_id >= 24
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.claimed_amount_plus_approved_amount,
        s.claim_type_status_label,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level < 3
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC) AS approved_amount_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_product ORDER BY adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_product, adjuster_id), 2) AS approved_amount_vs_policy_product_adjuster_id_av,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_product ORDER BY adjuster_level DESC, id) AS claimed_amount_change,
        NTILE(10) OVER (PARTITION BY policy_product ORDER BY adjuster_level DESC, id) AS adjuster_level_decile,
        FIRST_VALUE(policy_premium) OVER (PARTITION BY adjuster_level, policy_product ORDER BY claimed_amount_plus_approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_premium,
        NTILE(5) OVER w_adjuster_level_by_claim_date AS claim_date_quintile,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_team) AS adjuster_team_min_adjuster_level,
        NTILE(4) OVER w_adjuster_level_by_claim_date AS claim_date_quartile,
        SUM(adjuster_level) OVER (PARTITION BY adjuster_team) AS adjuster_team_sum_adjuster_level,
        COUNT(*) OVER (PARTITION BY policy_product, adjuster_id) AS rows_in_policy_product_adjuster_id,
        COUNT(*) OVER (PARTITION BY adjuster_team) AS rows_in_adjuster_team
    FROM with_details
    WINDOW w_adjuster_level_by_claim_date AS (PARTITION BY adjuster_level ORDER BY claim_date ASC, id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        claimed_amount_plus_approved_amount,
        claim_type_status_label,
        adjuster_name,
        adjuster_level,
        adjuster_team,
        policy_premium,
        policy_product,
        claimed_amount_avg_3,
        approved_amount_vs_policy_product_adjuster_id_av,
        claimed_amount_change,
        adjuster_level_decile,
        first_policy_premium,
        claim_date_quintile,
        adjuster_team_min_adjuster_level,
        claim_date_quartile,
        adjuster_team_sum_adjuster_level,
        rows_in_policy_product_adjuster_id,
        rows_in_adjuster_team
    FROM sequenced
    WHERE approved_amount_rank <= 5
      AND claimed_amount_plus_approved_amount < 60000.0
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount,
    approved_amount,
    adjuster_name,
    policy_premium,
    policy_product,
    claimed_amount_avg_3,
    approved_amount_vs_policy_product_adjuster_id_av,
    claimed_amount_change,
    first_policy_premium,
    adjuster_team_min_adjuster_level,
    claim_date_quartile,
    rows_in_adjuster_team,
    FLOOR(claimed_amount_change / 50000) * 50000 AS claimed_amount_change_bucket
FROM leaders
WHERE rows_in_policy_product_adjuster_id >= 8
ORDER BY claimed_amount_change
