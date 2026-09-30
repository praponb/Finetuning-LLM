WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        INLINE(items)
    FROM carts
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        qty,
        promo,
        SUM(qty) OVER (PARTITION BY customer_id, promo) AS customer_id_sum_qty,
        RANK() OVER (PARTITION BY promo, customer_id ORDER BY qty) AS qty_rank,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        PERCENT_RANK() OVER w_running AS qty_percent_rank
    FROM flattened
    WHERE price BETWEEN 210 AND 240
    WINDOW w_running AS (PARTITION BY promo ORDER BY qty NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(qty_share AS INT) AS qty_share_int,
        COALESCE(promo, 'DIRECT') AS promo_filled
    FROM with_windows
    WHERE customer_id_sum_qty IS NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        qty,
        promo,
        customer_id_sum_qty,
        qty_rank,
        qty_percent_rank,
        qty_share_int,
        ROUND(qty_percent_rank / qty, 2) AS qty_percent_rank_per_qty
    FROM base
    WHERE promo IS NOT NULL
      AND qty_share_int <= 62
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        qty_percent_rank * qty_share_int AS qty_percent_rank_times_qty_share_int,
        ROUND(qty_percent_rank_per_qty * 1.07, 2) AS qty_percent_rank_per_qty_adjusted,
        COALESCE(promo, 'UNKNOWN') AS promo_clean
    FROM valid
    WHERE promo IS NOT NULL
),
base_carts_2 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        qty,
        promo,
        customer_id_sum_qty,
        qty_rank,
        qty_percent_rank,
        qty_share_int,
        qty_percent_rank_per_qty,
        qty_percent_rank_times_qty_share_int,
        qty_percent_rank_per_qty_adjusted,
        promo_clean,
        LENGTH(promo) AS promo_length
    FROM base_carts
    WHERE promo IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    qty,
    customer_id_sum_qty,
    qty_share_int,
    qty_percent_rank_times_qty_share_int,
    qty_percent_rank_per_qty_adjusted,
    ROUND(qty_rank / SUM(qty_rank) OVER (PARTITION BY promo), 4) AS qty_rank_share
FROM base_carts_2
