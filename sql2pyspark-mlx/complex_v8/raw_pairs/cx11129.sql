WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.claimed_amount) AS prior_claimed_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
latest AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_90d,
        prior_claimed_amount_90d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY prior_rows_90d ORDER BY prior_claimed_amount_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        MAX(prior_rows_90d) OVER (PARTITION BY policy_id, YEAR(claim_date)) AS policy_id_claim_date_year_max_prior_rows_90d,
        RANK() OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY prior_rows_90d ASC NULLS LAST) AS prior_rows_90d_rank,
        MAX(prior_claimed_amount_90d) OVER (PARTITION BY policy_id, YEAR(claim_date)) AS policy_id_claim_date_year_max_prior_claimed_amou,
        COUNT(*) OVER (PARTITION BY policy_id, YEAR(claim_date)) AS rows_in_policy_id_claim_date_year,
        ROUND(prior_rows_90d / NULLIF(SUM(prior_rows_90d) OVER (PARTITION BY policy_id, YEAR(claim_date)), 0), 4) AS prior_rows_90d_share_of_policy_id_claim_date_yea
    FROM latest
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, prior_rows_90d_rank, policy_id_claim_date_year_max_prior_rows_90d
    FROM ranked
) src
UNPIVOT (
    reading FOR metric_name IN (prior_rows_90d_rank, policy_id_claim_date_year_max_prior_rows_90d)
) unp
ORDER BY id, metric_name
