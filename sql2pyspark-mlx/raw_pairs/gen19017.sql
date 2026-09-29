WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue NULLS FIRST, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, units - revenue AS revenue_per_unit
    FROM ranked
    WHERE units <= 12
),
ranked_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        sale_date,
        units,
        revenue,
        revenue_per_unit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units,
        LAG(units) OVER w_part AS prev_units,
        LEAD(units) OVER w_part AS next_units,
        LEAD(revenue) OVER w_part AS next_revenue,
        ROW_NUMBER() OVER w_part AS row_seq,
        FIRST_VALUE(revenue_per_unit) IGNORE NULLS OVER w_ordered AS first_known_revenue_per_unit
    FROM ranked_store_sales
    WHERE revenue > 3500
    WINDOW w_part AS (ORDER BY sale_date, id), w_ordered AS (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sale_date,
        units,
        prev_units,
        next_units,
        next_revenue,
        MAX(next_revenue) OVER w_part AS sale_date_period_max_next_revenue,
        LEAD(next_revenue, 2) OVER w_running AS next_next_revenue,
        LAG(row_seq, 1) OVER w_running AS prev_row_seq,
        LEAD(units, 2) OVER w_recent AS next_units_2
    FROM enriched
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('week', sale_date)), w_running AS (ORDER BY next_revenue, id), w_recent AS (ORDER BY sale_date, id)
)
SELECT
    id,
    units,
    prev_units,
    sale_date_period_max_next_revenue,
    next_next_revenue
FROM scored;
