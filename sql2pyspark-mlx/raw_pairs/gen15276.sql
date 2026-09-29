WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        unit_price,
        ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket,
        PERCENT_RANK() OVER (ORDER BY unit_price DESC NULLS LAST) AS unit_price_percent_rank,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        NTILE(5) OVER (ORDER BY unit_price_rounded, id) AS unit_price_rounded_bucket
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        unit_price,
        unit_price_rounded,
        unit_price_percent_rank,
        first_known_unit_price,
        unit_price_rounded_bucket,
        ROUND(unit_price_rounded_bucket * 0.9, 2) AS unit_price_rounded_bucket_scaled
    FROM with_windows
    WHERE unit_price_bucket <= 6
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity) OVER w_grp AS running_quantity,
        SUM(unit_price) OVER (ORDER BY first_known_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM cleaned
    WINDOW w_grp AS (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Ranking and gap features
SELECT
    id,
    quantity,
    unit_price_rounded,
    unit_price_percent_rank,
    unit_price_rounded_bucket,
    running_quantity,
    cumulative_unit_price,
    ROW_NUMBER() OVER w_hist AS event_seq,
    SUM(cumulative_unit_price) OVER (ORDER BY running_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_unit_price,
    LAST_VALUE(unit_price_percent_rank) IGNORE NULLS OVER (ORDER BY unit_price_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_percent_rank,
    ROW_NUMBER() OVER w_hist AS row_seq,
    LAST_VALUE(running_quantity, TRUE) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_quantity
FROM enriched
WINDOW w_hist AS (ORDER BY unit_price_rounded_bucket, id);
