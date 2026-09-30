WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE store_id IN (2, 4, 5)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share,
        ROW_NUMBER() OVER (ORDER BY revenue, id) AS seq_no
    FROM cleaned
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(units_share, 0) AS units_share_filled
    FROM scored
    WHERE seq_no > 3
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE revenue BETWEEN 2000 AND 3400
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        revenue,
        revenue_per_unit,
        units_share,
        seq_no,
        PERCENT_RANK() OVER (ORDER BY units_share DESC NULLS FIRST) AS units_share_percent_rank
    FROM kept_rows
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(units_share * 0.85, 2) AS adjusted_units_share,
        revenue_per_unit + revenue AS revenue_per_unit_plus_revenue
    FROM scored_store_sales
    WHERE units_share_percent_rank >= 0.51
),
cleaned_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        product_id,
        revenue,
        seq_no,
        revenue_per_unit_plus_revenue,
        seq_no * revenue_per_unit_plus_revenue AS seq_no_times_revenue_per_unit_plus_revenue
    FROM base
    WHERE units_share_percent_rank <= 0.47
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        CASE WHEN revenue >= 3800 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
        APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
        APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles_2,
        PERCENTILE_APPROX(seq_no, 0.95) AS seq_no_p95,
        PERCENT_RANK() OVER (PARTITION BY seq_no ORDER BY PERCENTILE_APPROX(seq_no, 0.95) DESC) AS seq_no_p95_percent_rank
    FROM cleaned_store_sales
    GROUP BY seq_no, CASE WHEN revenue >= 3800 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    seq_no,
    revenue_tier,
    seq_no_quartiles,
    seq_no_quartiles_2,
    seq_no_p95,
    seq_no_p95_percent_rank
FROM summary
ORDER BY seq_no_p95_percent_rank DESC NULLS LAST
