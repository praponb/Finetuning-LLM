WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount > 2569.2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        MAX(claimed_amount) OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claimed_amount_share_of_policy_id,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status), 2) AS approved_amount_vs_status_avg,
        LAST_VALUE(claimed_amount) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id), 2) AS claimed_amount_vs_adjuster_id_avg,
        SUM(claimed_amount) OVER w_adjuster_id AS adjuster_id_sum_claimed_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, adjuster_id ORDER BY approved_amount), 3) AS approved_amount_cume_dist
    FROM base
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        claimed_amount,
        status,
        reported_at,
        max_claimed_amount_so_far,
        last_known_reported_at,
        approved_amount_vs_status_avg,
        last_claimed_amount,
        claimed_amount_vs_adjuster_id_avg,
        approved_amount_cume_dist,
        QUARTER(reported_at) AS reported_at_quarter,
        LENGTH(TRIM(status)) AS status_len,
        last_claimed_amount - approved_amount AS last_claimed_amount_minus_approved_amount
    FROM with_windows
    WHERE max_claimed_amount_so_far >= 100080.0
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claimed_amount,
        status,
        max_claimed_amount_so_far,
        last_known_reported_at,
        approved_amount_vs_status_avg,
        last_claimed_amount,
        reported_at_quarter,
        status_len,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY YEAR(last_known_reported_at), status_len ORDER BY claimed_amount_vs_adjuster_id_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        LAG(reported_at_quarter, 3, 0) OVER (PARTITION BY MONTH(reported_at) ORDER BY approved_amount_vs_status_avg NULLS LAST, id) AS prev3_reported_at_quarter,
        NTH_VALUE(reported_at_quarter, 2) OVER (PARTITION BY MONTH(reported_at) ORDER BY approved_amount_vs_status_avg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_quarter,
        RANK() OVER w_all_by_claimed_amount AS claimed_amount_rank,
        RANK() OVER w_last_known_reported_at_year_2 AS claimed_amount_vs_adjuster_id_avg_rank,
        COUNT(*) OVER w_last_known_reported_at_month AS rows_in_last_known_reported_at_month,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount, id), 3) AS claimed_amount_cume_dist
    FROM tagged
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount, id), w_last_known_reported_at_year_2 AS (PARTITION BY YEAR(last_known_reported_at), status_len ORDER BY claimed_amount_vs_adjuster_id_avg ASC), w_last_known_reported_at_month AS (PARTITION BY MONTH(last_known_reported_at))
)
-- Final projection
SELECT
    id,
    claimed_amount,
    max_claimed_amount_so_far,
    last_known_reported_at,
    approved_amount_vs_status_avg,
    last_claimed_amount,
    reported_at_quarter,
    second_claimed_amount,
    prev3_reported_at_quarter,
    second_reported_at_quarter,
    claimed_amount_rank,
    claimed_amount_vs_adjuster_id_avg_rank,
    rows_in_last_known_reported_at_month,
    claimed_amount_cume_dist
FROM with_trends
WHERE status IN ('REJECTED', 'APPROVED', 'PAID')
ORDER BY reported_at_quarter, prev3_reported_at_quarter, id
LIMIT 5
