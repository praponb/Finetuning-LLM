WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product <> 'AUTO'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY holder_risk_score) AS holder_risk_score_rank,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        RANK() OVER (ORDER BY holder_risk_score, id) AS holder_risk_score_rank_2
    FROM without_matches
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE holder_risk_score_rank = 1
      AND holder_risk_score_rank_2 >= 4
)
SELECT
    id,
    end_date,
    status,
    holder_joined_on,
    holder_risk_score_rank,
    last_known_end_date,
    holder_risk_score_rank_2
FROM leaders
ORDER BY end_date DESC, last_known_end_date DESC NULLS FIRST
