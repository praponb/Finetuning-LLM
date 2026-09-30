WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE premium > 1042.8
      AND product IN ('HEALTH', 'TRAVEL', 'LIFE')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, status)) AS distinct_holder_id_in_product_status,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY days_start_date_to_end_date DESC, id) AS seq_no,
        ROUND(days_start_date_to_end_date / NULLIF(SUM(days_start_date_to_end_date) OVER (PARTITION BY product, status), 0), 4) AS days_start_date_to_end_date_share_of_product_sta,
        LAST_VALUE(start_date) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date
    FROM valid
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date
    FROM windowed s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM profiled
    WHERE product = 'LIFE'
)
-- Subtotals with ROLLUP
SELECT
    distinct_holder_id_in_product_status,
    policy_count_2,
    policy_count,
    GROUPING_ID() AS gid,
    MIN(end_date) AS first_end_date,
    COUNT(DISTINCT product) AS distinct_products
FROM derived
GROUP BY ROLLUP(distinct_holder_id_in_product_status, policy_count_2, policy_count)
