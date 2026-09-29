WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        COALESCE(revenue, 0) AS revenue_filled,
        COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE sale_date > DATE '2024-04-10'
      AND sale_date > '2024-12-01'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(revenue) AS revenue_whole
    FROM valid
    WHERE revenue_filled < 74
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.sqft,
        p.region,
        p.city
    FROM base s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        AVG(revenue_filled) OVER (ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_filled_avg_6_prior,
        LAST_VALUE(revenue_whole, TRUE) OVER w_hist AS last_known_revenue_whole,
        SUM(units) OVER w_recent AS running_units_2,
        LAG(revenue_whole, 1) OVER (PARTITION BY city ORDER BY revenue_filled, id) AS prev_revenue_whole
    FROM joined
    WINDOW w_hist AS (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY store_id ORDER BY revenue_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY running_units, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        AVG(revenue_filled_avg_6_prior) OVER (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_filled_avg_6_prior_avg_3,
        AVG(revenue_filled_avg_6_prior) OVER (PARTITION BY product_id ORDER BY prev_revenue_whole, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS revenue_filled_avg_6_prior_avg_2_prior
    FROM enriched
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched_store_sales
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE units BETWEEN 13 AND 38
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_store_sales s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    units,
    revenue,
    revenue_filled,
    revenue_whole,
    sqft,
    revenue_filled_avg_6_prior,
    last_known_revenue_whole,
    running_units_2
FROM kept_rows;
