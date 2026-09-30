WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        LAST_DAY(start_date) AS start_date_month_end,
        ABS(premium - 2096.6) AS premium_distance
    FROM policies
    WHERE product IN ('AUTO', 'HEALTH', 'TRAVEL')
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status <> 'OPEN'
              AND c.claim_date IS NOT NULL
        )
      AND s.status_prefix IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        status_prefix,
        start_date_month_end,
        premium_distance,
        ROW_NUMBER() OVER (ORDER BY premium_distance, id) AS premium_distance_seq,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, YEAR(start_date))) AS distinct_product_in_holder_id_start_date_year,
        NTILE(10) OVER (ORDER BY premium_distance, id) AS premium_distance_decile,
        ROUND(AVG(premium_distance) OVER (ORDER BY premium_distance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_6_prior
    FROM without_matches
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        start_date,
        status,
        status_prefix,
        start_date_month_end,
        premium_distance,
        premium_distance_decile,
        premium_distance_avg_6_prior,
        SIZE(COLLECT_SET(holder_id) OVER w_start_date_month_end_month_p) AS distinct_holder_id_in_start_date_month_end_month,
        NTILE(4) OVER w_all_by_distinct_product_in_h AS distinct_product_in_holder_id_start_date_year_qu,
        SUM(premium_distance) OVER (ORDER BY distinct_product_in_holder_id_start_date_year, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_distance_sum_2,
        MAX(premium_distance_decile) OVER (PARTITION BY holder_id ORDER BY distinct_product_in_holder_id_start_date_year ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_distance_decile_max_7,
        RANK() OVER w_holder_id_by_distinct_produc_2 AS distinct_product_in_holder_id_start_date_year_ra
    FROM with_trends
    WINDOW w_start_date_month_end_month_p AS (PARTITION BY MONTH(start_date_month_end), premium_distance_decile), w_all_by_distinct_product_in_h AS (ORDER BY distinct_product_in_holder_id_start_date_year, id), w_holder_id_by_distinct_produc_2 AS (PARTITION BY holder_id ORDER BY distinct_product_in_holder_id_start_date_year ASC, id)
),
with_trends_3 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_holder_id_in_start_date_month_end_month, status ORDER BY premium_distance_decile_max_7 DESC, id) AS premium_distance_decile_max_7_dense_rank,
        ROW_NUMBER() OVER w_distinct_holder_id_in_start AS seq_no
    FROM with_trends_2
    WINDOW w_distinct_holder_id_in_start AS (PARTITION BY distinct_holder_id_in_start_date_month_end_month, status ORDER BY premium_distance_decile_max_7 DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    status,
    status_prefix,
    start_date_month_end,
    premium_distance_decile,
    premium_distance_avg_6_prior,
    distinct_holder_id_in_start_date_month_end_month,
    distinct_product_in_holder_id_start_date_year_qu,
    premium_distance_sum_2,
    premium_distance_decile_max_7,
    premium_distance_decile_max_7_dense_rank
FROM with_trends_3
ORDER BY status_prefix DESC, premium_distance_decile
