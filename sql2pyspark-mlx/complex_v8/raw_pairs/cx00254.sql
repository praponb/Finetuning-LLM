WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek
    FROM claims
    WHERE claimed_amount < 31752.9
      AND approved_amount > 0.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        reported_at_dayofweek,
        ROUND(CUME_DIST() OVER w_adjuster_id_by_claim_date_ti, 3) AS claim_date_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek, id) AS reported_at_dayofweek_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek, id) AS reported_at_dayofweek_seq,
        ROUND(PERCENT_RANK() OVER w_claim_date_month_by_reported_2, 3) AS reported_at_dayofweek_percent_rank,
        SUM(reported_at_dayofweek) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS reported_at_dayofweek_sum_7,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY reported_at_dayofweek), 2) AS approved_amount_vs_reported_at_dayofweek_avg,
        LAG(reported_at_dayofweek) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id) AS prev_reported_at_dayofweek,
        ROUND(reported_at_dayofweek - AVG(reported_at_dayofweek) OVER (PARTITION BY reported_at_dayofweek), 2) AS reported_at_dayofweek_vs_reported_at_dayofweek_a,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS approved_amount_max_3_prior,
        ROUND(AVG(reported_at_dayofweek) OVER (PARTITION BY adjuster_id ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS reported_at_dayofweek_avg_3,
        SUM(reported_at_dayofweek) OVER (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS reported_at_dayofweek_sum_7_2,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY reported_at_dayofweek), 0), 4) AS approved_amount_share_of_reported_at_dayofweek,
        NTILE(10) OVER (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek, id) AS reported_at_dayofweek_decile,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount
    FROM base
    WINDOW w_adjuster_id_by_claim_date_ti AS (PARTITION BY adjuster_id ORDER BY claim_date DESC), w_claim_date_month_by_reported_2 AS (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek), w_claim_date_month_by_reported AS (PARTITION BY MONTH(claim_date) ORDER BY reported_at_dayofweek, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    reported_at_dayofweek,
    claim_date_cume_dist,
    reported_at_dayofweek_dense_rank,
    reported_at_dayofweek_percent_rank,
    reported_at_dayofweek_sum_7,
    approved_amount_vs_reported_at_dayofweek_avg,
    approved_amount_max_3_prior,
    reported_at_dayofweek_avg_3,
    reported_at_dayofweek_sum_7_2,
    reported_at_dayofweek_decile,
    running_approved_amount,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM with_windows
ORDER BY reported_at_dayofweek_avg_3
