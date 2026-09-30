WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        LOWER(TRIM(product)) AS product_clean,
        ABS(premium - 2289.0) AS premium_distance,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE status IS NOT NULL
      AND premium >= 2033.8
),
latest_per_key AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        product_clean,
        premium_distance,
        start_date_month_end
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium_distance DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IN ('flood', 'delay')
              AND HOUR(c.reported_at) BETWEEN 8 AND 14
        )
      AND s.holder_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        product_clean,
        premium_distance,
        start_date_month_end,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY status), 2) AS premium_distance_vs_status_avg,
        ROW_NUMBER() OVER (PARTITION BY YEAR(end_date) ORDER BY start_date_month_end ASC, id) AS rn,
        SIZE(COLLECT_SET(product) OVER w_status) AS distinct_product_in_status
    FROM with_activity
    WINDOW w_status AS (PARTITION BY status)
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium
    FROM windowed s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(premium_distance) OVER (PARTITION BY product, YEAR(end_date) ORDER BY rn, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_1_prior,
        ROW_NUMBER() OVER w_product_end_date_year_by_rn_2 AS rn_2,
        MAX(rn) OVER (PARTITION BY product, YEAR(end_date) ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rn_so_far,
        FIRST_VALUE(rn) IGNORE NULLS OVER (PARTITION BY product, YEAR(end_date) ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rn,
        ROUND(premium_distance / NULLIF(SUM(premium_distance) OVER w_product, 0), 4) AS premium_distance_share_of_product
    FROM summarized_rows
    WINDOW w_product_end_date_year_by_rn_2 AS (PARTITION BY product, YEAR(end_date) ORDER BY rn, id), w_product AS (PARTITION BY product)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        COALESCE(rn_2, 0) AS rn_2_filled
    FROM sequenced
)
-- Subtotals with GROUPING SETS
SELECT
    rn,
    status,
    rn_2,
    GROUPING_ID() AS gid,
    ROUND(SUM(rn) / NULLIF(SUM(rn_2_filled), 0), 3) AS rn_to_rn_2_filled_ratio
FROM enriched
GROUP BY GROUPING SETS ((rn, status, rn_2), (rn), (status), ())
ORDER BY rn NULLS LAST, status NULLS LAST, rn_2 NULLS LAST
