WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        DATE_TRUNC('year', last_restock) AS last_restock_period,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-08-06'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period NULLS FIRST) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        rank_in_group
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        rank_in_group,
        ROUND(rank_in_group * 1.15, 2) AS adjusted_rank_in_group
    FROM latest
    WHERE last_restock <= '2025-03-15'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, rank_in_group + adjusted_rank_in_group AS rank_in_group_plus_adjusted_rank_in_group
    FROM cleaned
    WHERE store_id IN (2, 3, 4, 6)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        rank_in_group,
        adjusted_rank_in_group,
        NTILE(4) OVER (ORDER BY adjusted_rank_in_group, id) AS adjusted_rank_in_group_bucket,
        SUM(rank_in_group_plus_adjusted_rank_in_group) OVER (ORDER BY adjusted_rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rank_in_group_plus_adjusted_rank_in_group
    FROM filtered
),
ranked_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY adjusted_rank_in_group_bucket DESC NULLS FIRST, id) AS latest_rank
        FROM with_windows
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    store_id,
    cumulative_rank_in_group_plus_adjusted_rank_in_group,
    latest_rank
FROM ranked_inventory
