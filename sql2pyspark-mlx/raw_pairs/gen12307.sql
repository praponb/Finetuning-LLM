WITH base AS (
    -- Keep recent, valid rows
    SELECT id, FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        unit_price_whole,
        ROW_NUMBER() OVER (ORDER BY unit_price_whole DESC, id) AS seq_no,
        SUM(unit_price_whole) OVER (ORDER BY unit_price_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_whole
    FROM base
),
base_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(unit_price_whole, 0) AS unit_price_whole_filled
    FROM with_windows
    WHERE seq_no < 5
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, seq_no - unit_price_whole_filled AS seq_no_minus_unit_price_whole_filled
    FROM base_order_items
    WHERE seq_no > 5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        seq_no,
        running_unit_price_whole,
        seq_no_minus_unit_price_whole_filled,
        AVG(unit_price_whole) OVER (ORDER BY seq_no_minus_unit_price_whole_filled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_whole_avg_5_prior,
        AVG(unit_price_whole_filled) OVER (ORDER BY unit_price_whole_filled, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_whole_filled_avg_3_prior
    FROM valid
    WHERE seq_no_minus_unit_price_whole_filled BETWEEN 46 AND 58
),
metrics_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(seq_no_minus_unit_price_whole_filled, TRUE) OVER w_recent AS last_known_seq_no_minus_unit_price_whole_filled,
        FIRST_VALUE(unit_price_whole_avg_5_prior, TRUE) OVER (ORDER BY unit_price_whole_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_whole_avg_5_prior,
        RANK() OVER (ORDER BY unit_price_whole_avg_5_prior DESC NULLS FIRST) AS unit_price_whole_avg_5_prior_rank,
        LAG(unit_price_whole_avg_5_prior, 1) OVER w_ordered AS prev_unit_price_whole_avg_5_prior
    FROM metrics
    WINDOW w_recent AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY seq_no_minus_unit_price_whole_filled, id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, last_known_seq_no_minus_unit_price_whole_filled + unit_price_whole_avg_5_prior AS last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior
    FROM metrics_order_items
    WHERE unit_price_whole_avg_5_prior < 59
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior,
        last_known_seq_no_minus_unit_price_whole_filled
    FROM cleaned
) src
UNPIVOT (
    reading FOR metric IN (last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior, last_known_seq_no_minus_unit_price_whole_filled)
)
