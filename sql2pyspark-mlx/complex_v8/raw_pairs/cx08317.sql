WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 11
      AND adjuster_id <= 5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(claimed_amount) OVER (PARTITION BY status) AS status_min_claimed_amount,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status), 2) AS status_avg_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY status) AS status_max_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_max_7,
        LAG(reported_at) OVER w_all_by_approved_amount AS prev_reported_at,
        NTILE(4) OVER w_adjuster_id_reported_at_mont_3 AS reported_at_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_cume_dist
    FROM filtered_claims
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id), w_adjuster_id_reported_at_mont_3 AS (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at) ORDER BY reported_at, id), w_status AS (PARTITION BY status)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at_plus_1d,
        status_min_claimed_amount,
        rows_in_status,
        last_known_claim_date,
        status_avg_claimed_amount,
        status_max_claimed_amount,
        claimed_amount_vs_status_avg,
        prev_reported_at,
        reported_at_quartile,
        reported_at_percent_rank,
        approved_amount_cume_dist,
        CONCAT(status, '#', CAST(reported_at_quartile AS STRING)) AS status_ref,
        LEAST(claimed_amount, reported_at_percent_rank) AS least_claimed_amount_reported_at_percent_rank
    FROM sequenced
    WHERE status = 'OPEN'
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        status_min_claimed_amount,
        rows_in_status,
        last_known_claim_date,
        status_avg_claimed_amount,
        status_max_claimed_amount,
        claimed_amount_vs_status_avg,
        reported_at_percent_rank,
        approved_amount_cume_dist,
        status_ref,
        least_claimed_amount_reported_at_percent_rank,
        ROUND(AVG(status_min_claimed_amount) OVER (PARTITION BY status ORDER BY least_claimed_amount_reported_at_percent_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS status_min_claimed_amount_avg_4,
        LAG(reported_at_quartile) OVER (ORDER BY status_min_claimed_amount ASC, id) AS prev_reported_at_quartile,
        rows_in_status - LAG(rows_in_status) OVER w_status_by_least_claimed_amou_2 AS rows_in_status_change,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY least_claimed_amount_reported_at_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(AVG(rows_in_status) OVER (PARTITION BY status ORDER BY least_claimed_amount_reported_at_percent_rank DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rows_in_status_avg_7,
        NTILE(5) OVER w_all_by_status_min_claimed_am AS status_min_claimed_amount_quintile,
        ROUND(AVG(status_max_claimed_amount) OVER (PARTITION BY reported_at_quartile), 2) AS reported_at_quartile_avg_status_max_claimed_amou,
        ROUND(CUME_DIST() OVER w_status_by_least_claimed_amou_2, 3) AS least_claimed_amount_reported_at_percent_rank_cu
    FROM enriched
    WINDOW w_status_by_least_claimed_amou_2 AS (PARTITION BY status ORDER BY least_claimed_amount_reported_at_percent_rank DESC, id), w_all_by_status_min_claimed_am AS (ORDER BY status_min_claimed_amount ASC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    rows_in_status,
    last_known_claim_date,
    status_max_claimed_amount,
    claimed_amount_vs_status_avg,
    reported_at_percent_rank,
    approved_amount_cume_dist,
    status_ref,
    least_claimed_amount_reported_at_percent_rank,
    status_min_claimed_amount_avg_4,
    prev_reported_at_quartile,
    rows_in_status_change,
    first_known_claim_date,
    rows_in_status_avg_7,
    status_min_claimed_amount_quintile,
    reported_at_quartile_avg_status_max_claimed_amou,
    least_claimed_amount_reported_at_percent_rank_cu,
    DAYOFWEEK(claim_date) AS claim_date_dayofweek
FROM sequenced_2
WHERE status <> 'IN_REVIEW'
ORDER BY claim_date_dayofweek, id
LIMIT 5
