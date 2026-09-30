WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_type IS NOT NULL
      AND policy_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER w_policy_id_claim_date_month) AS distinct_claim_type_in_policy_id_claim_date_mont,
        ROW_NUMBER() OVER w_policy_id_by_reported_at_dat AS reported_at_date_seq,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY reported_at_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_6_prior,
        SUM(claimed_amount) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)) AS policy_id_claim_date_month_sum_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, YEAR(reported_at) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        COUNT(*) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)) AS rows_in_policy_id_claim_date_month
    FROM valid_claims
    WINDOW w_policy_id_claim_date_month AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)), w_policy_id_by_reported_at_dat AS (PARTITION BY policy_id ORDER BY reported_at_date NULLS LAST, id)
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.distinct_claim_type_in_policy_id_claim_date_mont,
        s.approved_amount_avg_6_prior,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM with_trends s
    JOIN claims c ON c.policy_id = s.policy_id AND c.status <> 'IN_REVIEW'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        claim_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        claim_status,
        COUNT(*) AS row_count,
        ROUND(STDDEV(approved_amount_avg_6_prior), 2) AS stddev_approved_amount_avg_6_prior,
        SUM(distinct_claim_type_in_policy_id_claim_date_mont) AS total_distinct_claim_type_in_policy_id_claim_dat,
        COUNT(DISTINCT claim_id) AS distinct_claim_ids,
        MAX(claimed_amount) AS max_claimed_amount,
        MAX(claim_date) AS last_claim_date
    FROM line_level
    GROUP BY claim_id, DATE_FORMAT(claim_date, 'yyyy-MM'), claim_status
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        claim_id,
        claim_date_ym,
        claim_status,
        row_count,
        distinct_claim_ids,
        max_claimed_amount,
        last_claim_date,
        LAST_VALUE(row_count) OVER (PARTITION BY row_count ORDER BY max_claimed_amount ASC, claim_id, claim_date_ym, claim_status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_row_count,
        FIRST_VALUE(max_claimed_amount) IGNORE NULLS OVER (ORDER BY max_claimed_amount DESC, claim_id, claim_date_ym, claim_status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_claimed_amount,
        MIN(row_count) OVER (PARTITION BY row_count) AS row_count_min_row_count,
        LEAD(distinct_claim_ids, 1, 0) OVER (PARTITION BY row_count ORDER BY max_claimed_amount ASC, claim_id, claim_date_ym, claim_status) AS next_distinct_claim_ids,
        LEAD(row_count) OVER (ORDER BY max_claimed_amount DESC, claim_id, claim_date_ym, claim_status) AS next_row_count,
        ROUND(row_count / NULLIF(SUM(row_count) OVER w_row_count, 0), 4) AS row_count_share_of_row_count
    FROM rollup_base
    WINDOW w_row_count AS (PARTITION BY row_count)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(claim_date_ym), 1, 4) AS claim_date_ym_prefix,
        ABS(last_row_count - 7) AS last_row_count_distance
    FROM sequenced
    WHERE last_row_count < 10
)
SELECT
    claim_id,
    claim_status,
    row_count,
    last_claim_date,
    row_count_min_row_count,
    claim_date_ym_prefix,
    last_row_count_distance,
    SUBSTRING(TRIM(claim_date_ym), 1, 4) AS claim_date_ym_prefix_2,
    ROUND(next_row_count / NULLIF(distinct_claim_ids, 0), 3) AS next_row_count_per_distinct_claim_ids
FROM tagged
