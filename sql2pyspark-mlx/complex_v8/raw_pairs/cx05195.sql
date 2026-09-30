WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claimed_amount < 40639.7
      AND reported_at < TIMESTAMP '2025-02-18 13:00:00'
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.greatest_approved_amount_claimed_amount,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM prepared_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IS NOT NULL
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        greatest_approved_amount_claimed_amount,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY claim_type, adjuster_level ORDER BY reported_at ASC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROW_NUMBER() OVER w_reported_at_to_date_policy_i AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_level ORDER BY reported_at ASC, id) AS reported_at_seq,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY approved_amount DESC, id) AS approved_amount_dense_rank,
        ROUND(CUME_DIST() OVER w_adjuster_id_by_approved_amou, 3) AS approved_amount_cume_dist,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY claim_type, adjuster_level ORDER BY reported_at ASC, id) AS claimed_amount_change,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_claimed_amount,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at
    FROM joined
    WINDOW w_reported_at_to_date_policy_i AS (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY approved_amount DESC, id), w_adjuster_id_by_approved_amou AS (PARTITION BY adjuster_id ORDER BY approved_amount ASC, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        DATE_FORMAT(second_reported_at, 'yyyy-MM') AS second_reported_at_ym,
        approved_amount_dense_rank,
        ROUND(STDDEV(seq_no), 2) AS stddev_seq_no,
        ROUND(SUM(approved_amount_cume_dist) / NULLIF(SUM(greatest_approved_amount_claimed_amount), 0), 3) AS approved_amount_cume_dist_to_greatest_approved_a,
        MAX(second_reported_at) AS last_second_reported_at,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM with_windows
    GROUP BY adjuster_id, DATE_FORMAT(second_reported_at, 'yyyy-MM'), approved_amount_dense_rank
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(distinct_adjuster_ids) OVER (ORDER BY approved_amount_cume_dist_to_greatest_approved_a, adjuster_id, second_reported_at_ym, approved_amount_dense_rank ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_adjuster_ids_avg_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows ORDER BY stddev_seq_no NULLS LAST), 3) AS stddev_seq_no_cume_dist,
        LAST_VALUE(last_second_reported_at) IGNORE NULLS OVER (PARTITION BY n_rows ORDER BY stddev_seq_no NULLS LAST, adjuster_id, second_reported_at_ym, approved_amount_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_second_reported_at,
        RANK() OVER (PARTITION BY n_rows ORDER BY stddev_seq_no NULLS LAST, adjuster_id, second_reported_at_ym, approved_amount_dense_rank) AS stddev_seq_no_rank
    FROM grouped
)
SELECT
    adjuster_id,
    second_reported_at_ym,
    stddev_seq_no,
    distinct_adjuster_ids,
    distinct_adjuster_ids_avg_1_prior,
    stddev_seq_no_cume_dist,
    FLOOR(stddev_seq_no / 2) * 2 AS stddev_seq_no_bucket
FROM with_trends
ORDER BY stddev_seq_no DESC NULLS FIRST, second_reported_at_ym DESC
