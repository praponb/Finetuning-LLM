WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        WEEKOFYEAR(last_restock) AS last_restock_weekofyear
    FROM inventory
    WHERE store_id IN (1, 2, 3, 6)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock_weekofyear
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_weekofyear ASC, id ASC) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC) AS rank_in_group
        FROM deduped
    ) t
    WHERE rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_weekofyear ASC, id ASC) AS rank_in_group_2
        FROM ranked
    ) x
    WHERE rank_in_group_2 = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock_weekofyear ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rank_in_group DESC, id ASC) AS row_seq,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY last_restock_weekofyear DESC, id ASC) AS last_restock_weekofyear_bucket,
        FIRST_VALUE(quantity, TRUE) OVER (ORDER BY rank_in_group_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        MIN(quantity) OVER (PARTITION BY product_id) AS product_id_min_quantity
    FROM latest
    WHERE rank_in_group_2 <= 1
),
deduped_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id_min_quantity DESC, id ASC) AS rank_in_group_3
        FROM enriched
    ) t
    WHERE rank_in_group_3 = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock_weekofyear,
        s.rank_in_group,
        s.last_restock_weekofyear_bucket,
        p.product_name,
        p.launch_date
    FROM deduped_inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last_restock_weekofyear >= 84 THEN 'HIGH' ELSE 'LOW' END AS last_restock_weekofyear_tier,
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        ROUND(AVG(last_restock_weekofyear_bucket), 2) AS avg_last_restock_weekofyear_bucket,
        PERCENTILE_APPROX(last_restock_weekofyear, 0.5) AS last_restock_weekofyear_p50,
        COUNT(DISTINCT id) AS distinct_ids,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN last_restock_weekofyear >= 84 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(AVG(last_restock_weekofyear_bucket), 2) DESC) AS avg_last_restock_weekofyear_bucket_dense_rank
    FROM with_dims
    GROUP BY CASE WHEN last_restock_weekofyear >= 84 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', launch_date)
)
SELECT
    last_restock_weekofyear_tier,
    week_start,
    avg_last_restock_weekofyear_bucket,
    last_restock_weekofyear_p50,
    distinct_ids
FROM daily
ORDER BY avg_last_restock_weekofyear_bucket DESC NULLS LAST
