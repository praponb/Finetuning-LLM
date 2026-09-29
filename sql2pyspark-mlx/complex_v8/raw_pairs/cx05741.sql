WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM claims
    WHERE approved_amount > 492.6
),
policy_id_stats AS (
    -- Median claimed_amount per policy_id
    SELECT policy_id, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM filtered
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM filtered s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.claimed_amount >= g.median_claimed_amount
),
rollup_base AS (
    -- Group-level totals
    SELECT
        claim_type_len,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(*) AS cnt,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM vs_group
    GROUP BY claim_type_len
),
labeled AS (
    -- Derive helper columns
    SELECT *, FLOOR(distinct_policy_ids / 2) * 2 AS distinct_policy_ids_bucket
    FROM rollup_base
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(distinct_status) OVER (PARTITION BY cnt, distinct_policy_ids_bucket) AS cnt_distinct_policy_ids_bucket_max_distinct_stat,
        ROUND(AVG(distinct_status) OVER w_cnt_distinct_policy_ids_buck, 2) AS cnt_distinct_policy_ids_bucket_avg_distinct_stat
    FROM labeled
    WINDOW w_cnt_distinct_policy_ids_buck AS (PARTITION BY cnt, distinct_policy_ids_bucket)
),
scored AS (
    -- Derive helper columns
    SELECT
        claim_type_len,
        distinct_policy_ids,
        cnt_distinct_policy_ids_bucket_max_distinct_stat,
        cnt_distinct_policy_ids_bucket_avg_distinct_stat,
        ABS(distinct_policy_ids - 5) AS distinct_policy_ids_distance,
        FLOOR(cnt / 2) * 2 AS cnt_bucket,
        distinct_status + claim_type_len AS distinct_status_plus_claim_type_len
    FROM ranked
)
-- Final projection
SELECT
    claim_type_len,
    cnt_distinct_policy_ids_bucket_max_distinct_stat,
    cnt_distinct_policy_ids_bucket_avg_distinct_stat,
    distinct_policy_ids_distance,
    cnt_bucket,
    distinct_status_plus_claim_type_len,
    cnt_bucket + distinct_policy_ids AS cnt_bucket_plus_distinct_policy_ids
FROM scored
ORDER BY cnt_distinct_policy_ids_bucket_avg_distinct_stat DESC, distinct_policy_ids_distance NULLS LAST
