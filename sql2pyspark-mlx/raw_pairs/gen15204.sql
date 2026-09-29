WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        brand,
        cost,
        launch_date,
        tags,
        supplier_id,
        UPPER(brand) AS brand_upper,
        LAST_DAY(launch_date) AS launch_date_month_end,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM products
    WHERE launch_date >= DATE '2023-05-20'
      AND brand IS DISTINCT FROM 'Acme'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        brand,
        cost,
        launch_date,
        tags,
        launch_date_month_end,
        resolved_brand,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) AS cost_share,
        RANK() OVER (PARTITION BY brand, supplier_id ORDER BY cost DESC NULLS LAST) AS cost_rank
    FROM valid
    WHERE launch_date > '2023-04-01'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY brand ORDER BY cost DESC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost_share) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_cost_share,
        MIN(cost_share) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_cost_share,
        MAX(cost_share) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_cost_share
    FROM ranked
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(last3_max_cost_share) OVER w_recent AS last3_min_last3_max_cost_share,
        SUM(last3_max_cost_share) OVER w_recent AS last3_sum_last3_max_cost_share,
        MAX(last3_max_cost_share) OVER w_recent AS last3_max_last3_max_cost_share,
        COUNT(last3_max_cost_share) OVER w_recent AS last3_count_last3_max_cost_share,
        ROW_NUMBER() OVER (PARTITION BY YEAR(launch_date) ORDER BY last3_max_cost_share DESC, id) AS event_seq
    FROM scored
    WINDOW w_recent AS (PARTITION BY brand ORDER BY last3_sum_cost_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    brand,
    cost,
    launch_date,
    tags,
    resolved_brand,
    cost_share,
    cost_rank,
    latest_rank
FROM scored_products
ORDER BY brand DESC
