WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE claim_date < DATE '2025-03-31'
      AND reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.approved_amount_filled,
        s.status_slug,
        a.adjuster_name
    FROM valid s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_adjuster_id_by_approved_amou AS rn,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        NTH_VALUE(approved_amount_filled, 2) OVER (PARTITION BY status, claim_type ORDER BY approved_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount_filled,
        ROUND(AVG(approved_amount_filled) OVER (PARTITION BY adjuster_id ORDER BY approved_amount_filled ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_filled_avg_4,
        ROW_NUMBER() OVER (ORDER BY approved_amount_filled DESC, id) AS rn_2,
        approved_amount_filled - LAG(approved_amount_filled) OVER w_status_claim_type_by_approve_2 AS approved_amount_filled_change,
        ROUND(AVG(approved_amount_filled) OVER w_policy_id, 2) AS policy_id_avg_approved_amount_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount), 3) AS approved_amount_percent_rank,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        SUM(approved_amount_filled) OVER (PARTITION BY adjuster_id ORDER BY approved_amount_filled ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_filled_sum_6_prior,
        LEAD(approved_amount) OVER w_status_claim_type_by_approve_2 AS next_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount_filled DESC, id), 3) AS approved_amount_filled_cume_dist,
        LEAD(claim_date, 3) OVER (PARTITION BY policy_id ORDER BY approved_amount_filled DESC, id) AS next3_claim_date,
        MAX(approved_amount) OVER (PARTITION BY YEAR(claim_date) ORDER BY approved_amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_max_6_prior
    FROM enriched_adjuster
    WINDOW w_adjuster_id_by_approved_amou AS (PARTITION BY adjuster_id ORDER BY approved_amount_filled ASC, id), w_status_claim_type_by_approve_2 AS (PARTITION BY status, claim_type ORDER BY approved_amount_filled DESC, id), w_policy_id AS (PARTITION BY policy_id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE rn_2 = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    status,
    approved_amount_filled,
    status_slug,
    adjuster_name,
    rn,
    second_approved_amount_filled,
    approved_amount_filled_avg_4,
    rn_2,
    policy_id_avg_approved_amount_filled,
    first_approved_amount,
    approved_amount_share_of_policy_id,
    approved_amount_filled_sum_6_prior,
    next_approved_amount,
    approved_amount_filled_cume_dist,
    approved_amount_max_6_prior,
    SUBSTRING(TRIM(adjuster_name), 1, 2) AS adjuster_name_prefix,
    TRUNC(claim_date, 'MM') AS claim_date_month_start
FROM first_rows
ORDER BY approved_amount_filled_avg_4, next_approved_amount DESC NULLS FIRST, id
LIMIT 5
