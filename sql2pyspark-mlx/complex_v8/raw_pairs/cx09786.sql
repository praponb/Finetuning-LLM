WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        HOUR(reported_at) AS reported_at_hour,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE status <> 'IN_REVIEW'
      AND MONTH(reported_at) IN (1, 3)
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE status_len < (SELECT MAX(status_len) FROM screened)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_reported_at_month_by_claim_d, 3) AS claim_date_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC NULLS FIRST, id), 3) AS claim_date_cume_dist_2,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY claim_type), 2) AS status_len_vs_claim_type_avg
    FROM without_matches
    WINDOW w_reported_at_month_by_claim_d AS (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY claim_date)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status, adjuster_id ORDER BY reported_at) AS reported_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY status, adjuster_id ORDER BY reported_at), 3) AS reported_at_cume_dist,
        LEAD(status_len_vs_claim_type_avg, 3) OVER (PARTITION BY status, adjuster_id ORDER BY reported_at, id) AS next3_status_len_vs_claim_type_avg,
        COUNT(*) OVER w_status_status_len AS rows_in_status_status_len,
        LAG(reported_at_hour, 1, 0) OVER (PARTITION BY status_len ORDER BY claim_date, id) AS prev_reported_at_hour
    FROM windowed
    WINDOW w_status_status_len AS (PARTITION BY status, status_len)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at_hour,
        claim_date_cume_dist,
        claim_date_cume_dist_2,
        status_len_vs_claim_type_avg,
        reported_at_dense_rank,
        next3_status_len_vs_claim_type_avg,
        rows_in_status_status_len,
        prev_reported_at_hour,
        YEAR(claim_date) AS claim_date_year
    FROM with_trends
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, claimed_amount, claim_date_cume_dist_2
    FROM enriched
) src
UNPIVOT (
    metric_value FOR field IN (claimed_amount, claim_date_cume_dist_2)
) unp
