WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        LAST_DAY(last_restock) AS last_restock_month_end,
        MONTH(last_restock) AS last_restock_month
    FROM inventory
    WHERE quantity = 187
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        quantity,
        last_restock_month_end,
        last_restock_month,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST) AS quantity_cume_dist,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY last_restock_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
        SUM(quantity) OVER (ORDER BY last_restock_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM cleaned
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY first_known_quantity NULLS LAST, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
latest_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_avg_2 NULLS LAST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity_avg_2) OVER (PARTITION BY store_id) AS store_id_sum_quantity_avg_2,
        FIRST_VALUE(quantity_cume_dist) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_quantity, id) AS event_seq,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY running_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_2
    FROM latest_inventory
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        BIT_AND(quantity) AS quantity_bit_and,
        APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
        BIT_OR(quantity) AS quantity_bit_or
    FROM scored
    GROUP BY 1
)
SELECT
    store_id,
    quantity_bit_and,
    quantity_p95
FROM stats
ORDER BY quantity_p95 DESC NULLS FIRST
