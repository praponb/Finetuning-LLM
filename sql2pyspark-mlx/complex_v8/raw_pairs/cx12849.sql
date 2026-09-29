WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at
    FROM claims
    WHERE approved_amount < 32510.7
      AND claim_date < DATE '2025-04-17'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_date,
        claim_type,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        RANK() OVER (ORDER BY reported_at DESC) AS reported_at_rank,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at
    FROM cleaned
),
summary AS (
    -- Summarise per group
    SELECT
        claim_type,
        reported_at_rank,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS n_claim_type_is_not_null,
        SUM(reported_at_percent_rank) AS total_reported_at_percent_rank,
        MAX(reported_at_percent_rank) AS max_reported_at_percent_rank,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM ranked
    GROUP BY 1, 2, 3
),
per_label AS (
    -- One row per claim_type_list_item
    SELECT
        s.claim_type,
        s.reported_at_rank,
        s.claim_date_month,
        s.distinct_claim_types,
        s.n_claim_type_is_not_null,
        s.total_reported_at_percent_rank,
        s.max_reported_at_percent_rank,
        t.claim_type_list_item
    FROM summary s
    LATERAL VIEW EXPLODE(s.claim_type_list) t AS claim_type_list_item
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_label
    WHERE n_claim_type_is_not_null > (SELECT AVG(n_claim_type_is_not_null) FROM per_label)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        claim_type,
        reported_at_rank,
        claim_date_month,
        distinct_claim_types,
        n_claim_type_is_not_null,
        max_reported_at_percent_rank,
        claim_type_list_item,
        LAG(claim_date_month, 3) OVER (PARTITION BY reported_at_rank, claim_type_list_item ORDER BY distinct_claim_types DESC, claim_type, reported_at_rank, claim_date_month, claim_type_list_item) AS prev3_claim_date_month,
        ROUND(distinct_claim_types / NULLIF(SUM(distinct_claim_types) OVER (PARTITION BY claim_type), 0), 4) AS distinct_claim_types_share_of_claim_type,
        LEAD(n_claim_type_is_not_null) OVER (PARTITION BY n_claim_type_is_not_null, DATE_TRUNC('MONTH', claim_date_month) ORDER BY n_claim_type_is_not_null, claim_type, reported_at_rank, claim_date_month, claim_type_list_item) AS next_n_claim_type_is_not_null,
        LAST_VALUE(n_claim_type_is_not_null) IGNORE NULLS OVER (PARTITION BY claim_type, claim_type_list_item ORDER BY distinct_claim_types DESC, claim_type, reported_at_rank, claim_date_month, claim_type_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_claim_type_is_not_null,
        NTILE(10) OVER w_reported_at_rank_claim_type AS distinct_claim_types_decile
    FROM screened
    WINDOW w_reported_at_rank_claim_type AS (PARTITION BY reported_at_rank, claim_type_list_item ORDER BY distinct_claim_types DESC, claim_type, reported_at_rank, claim_date_month, claim_type_list_item)
),
scored AS (
    -- Label and score rows
    SELECT
        claim_type,
        reported_at_rank,
        claim_date_month,
        n_claim_type_is_not_null,
        claim_type_list_item,
        prev3_claim_date_month,
        distinct_claim_types_share_of_claim_type,
        next_n_claim_type_is_not_null,
        last_known_n_claim_type_is_not_null,
        distinct_claim_types_decile,
        ABS(distinct_claim_types - 6) AS distinct_claim_types_distance,
        CONCAT(claim_type, '#', CAST(distinct_claim_types AS STRING)) AS claim_type_ref
    FROM windowed
)
SELECT
    next_n_claim_type_is_not_null,
    claim_type,
    MIN(reported_at_rank) AS min_reported_at_rank,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ROUND(AVG(distinct_claim_types_share_of_claim_type), 2) AS avg_distinct_claim_types_share_of_claim_type
FROM scored
GROUP BY ROLLUP(next_n_claim_type_is_not_null, claim_type)
