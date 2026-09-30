WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 32796.4 THEN 'A' WHEN claimed_amount >= 7204.1 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        ADD_MONTHS(claim_date, 3) AS claim_date_plus_3m,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj
    FROM claims
    WHERE adjuster_id >= 7
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claimed_amount_adj / NULLIF(SUM(claimed_amount_adj) OVER (PARTITION BY YEAR(claim_date)), 0), 4) AS claimed_amount_adj_share_of_claim_date_year,
        MAX(claimed_amount) OVER w_claim_date_year AS claim_date_year_max_claimed_amount
    FROM valid
    WINDOW w_claim_date_year AS (PARTITION BY YEAR(claim_date))
),
policy_id_stats AS (
    -- Max claimed_amount_adj per policy_id
    SELECT policy_id, MAX(claimed_amount_adj) AS max_claimed_amount_adj
    FROM windowed
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_claimed_amount_adj,
        ROUND(s.claimed_amount_adj - g.max_claimed_amount_adj, 2) AS claimed_amount_adj_minus_max
    FROM windowed s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.claimed_amount_adj = g.max_claimed_amount_adj
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    claim_date_plus_3m,
    claimed_amount_adj,
    max_claimed_amount_adj,
    claimed_amount_adj_minus_max,
    CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
FROM above_average
WHERE max_claimed_amount_adj > 11335.0
ORDER BY id NULLS LAST
