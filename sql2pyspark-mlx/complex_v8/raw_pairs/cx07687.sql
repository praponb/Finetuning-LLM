WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek
    FROM claims
    WHERE status = 'IN_REVIEW'
      AND policy_id >= 16
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type)) AS distinct_policy_id_in_claim_type,
        MIN(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_min_claimed_amount,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY adjuster_id, reported_at_dayofweek ORDER BY reported_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        SUM(reported_at_dayofweek) OVER (PARTITION BY claim_type, MONTH(reported_at) ORDER BY reported_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS reported_at_dayofweek_sum_1_prior,
        MIN(reported_at_dayofweek) OVER (PARTITION BY claim_type) AS claim_type_min_reported_at_dayofweek,
        ROW_NUMBER() OVER w_all_by_reported_at_dayofweek AS reported_at_dayofweek_seq,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_claimed_amount,
        SUM(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount
    FROM valid_claims
    WINDOW w_all_by_reported_at_dayofweek AS (ORDER BY reported_at_dayofweek NULLS LAST, id)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        reported_at_dayofweek,
        distinct_policy_id_in_claim_type,
        second_claim_date,
        reported_at_dayofweek_sum_1_prior,
        claim_type_min_reported_at_dayofweek,
        reported_at_dayofweek_seq,
        adjuster_id_sum_claimed_amount,
        running_claimed_amount,
        RANK() OVER (PARTITION BY claim_type ORDER BY claim_date DESC) AS claim_date_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        reported_at_dayofweek - LAG(reported_at_dayofweek) OVER (PARTITION BY distinct_policy_id_in_claim_type, claim_type ORDER BY adjuster_id_sum_claimed_amount, id) AS reported_at_dayofweek_change,
        MAX(claimed_amount) OVER (PARTITION BY reported_at_dayofweek_seq) AS reported_at_dayofweek_seq_max_claimed_amount
    FROM sequenced
),
per_group AS (
    -- Group-level totals
    SELECT
        distinct_policy_id_in_claim_type,
        DATE_FORMAT(second_claim_date, 'yyyy-MM') AS second_claim_date_ym,
        reported_at_dayofweek_seq,
        SUM(CASE WHEN claim_type = 'flood' THEN 1 ELSE 0 END) AS n_claim_type_flood,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced_2
    GROUP BY distinct_policy_id_in_claim_type, DATE_FORMAT(second_claim_date, 'yyyy-MM'), reported_at_dayofweek_seq
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        distinct_policy_id_in_claim_type,
        second_claim_date_ym,
        reported_at_dayofweek_seq,
        n_claim_type_flood,
        distinct_ids,
        SUM(distinct_ids) OVER (PARTITION BY n_claim_type_flood, distinct_policy_id_in_claim_type) AS n_claim_type_flood_distinct_policy_id_in_claim_t,
        FIRST_VALUE(n_claim_type_flood) IGNORE NULLS OVER (PARTITION BY distinct_policy_id_in_claim_type, n_claim_type_flood ORDER BY n_claim_type_flood ASC NULLS LAST, distinct_policy_id_in_claim_type, second_claim_date_ym, reported_at_dayofweek_seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_claim_type_flood,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_policy_id_in_claim_type, n_claim_type_flood ORDER BY n_claim_type_flood ASC NULLS LAST, distinct_policy_id_in_claim_type, second_claim_date_ym, reported_at_dayofweek_seq), 3) AS n_claim_type_flood_cume_dist,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY distinct_policy_id_in_claim_type, n_claim_type_flood ORDER BY n_claim_type_flood ASC NULLS LAST, distinct_policy_id_in_claim_type, second_claim_date_ym, reported_at_dayofweek_seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids
    FROM per_group
)
-- Final projection
SELECT
    distinct_policy_id_in_claim_type,
    reported_at_dayofweek_seq,
    n_claim_type_flood,
    n_claim_type_flood_distinct_policy_id_in_claim_t,
    first_known_n_claim_type_flood,
    n_claim_type_flood_cume_dist,
    second_distinct_ids,
    FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket
FROM windowed
ORDER BY n_claim_type_flood DESC, distinct_policy_id_in_claim_type DESC
