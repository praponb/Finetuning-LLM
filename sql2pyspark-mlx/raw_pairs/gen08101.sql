WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        last_restock,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-09-17'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(last_restock, 64) AS last_restock_minus_64d
    FROM valid
    WHERE last_restock >= DATE '2023-04-24'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY quantity_whole DESC, id ASC) AS quantity_whole_bucket,
        AVG(quantity_whole) OVER (ORDER BY last_restock ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_whole_avg_2_prior,
        FIRST_VALUE(quantity_whole, TRUE) OVER (ORDER BY quantity_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_whole,
        LEAD(quantity_whole, 2) OVER (PARTITION BY MONTH(last_restock) ORDER BY last_restock_minus_64d ASC, id ASC) AS next_quantity_whole
    FROM base
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity_whole_bucket, 1, 0) OVER (ORDER BY quantity_whole_avg_2_prior ASC, id ASC) AS next_quantity_whole_bucket,
        AVG(first_known_quantity_whole) OVER (ORDER BY quantity_whole_avg_2_prior ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS first_known_quantity_whole_avg_3_prior
    FROM scored
),
base_inventory AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(next_quantity_whole_bucket) AS next_quantity_whole_bucket_whole
    FROM scored_inventory
    WHERE quantity_whole > 34
),
scored_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS quantity_whole_avg_2_prior_percent_rank,
        PERCENT_RANK() OVER w_grp AS quantity_whole_bucket_percent_rank,
        LAG(next_quantity_whole_bucket, 2) OVER (ORDER BY next_quantity_whole_bucket ASC, id ASC) AS prev_next_quantity_whole_bucket
    FROM base_inventory
    WINDOW w_ordered AS (ORDER BY quantity_whole_avg_2_prior ASC NULLS LAST), w_grp AS (ORDER BY quantity_whole_bucket DESC)
)
SELECT
    id,
    last_restock,
    quantity_whole,
    last_restock_minus_64d,
    next_quantity_whole,
    first_known_quantity_whole_avg_3_prior,
    next_quantity_whole_bucket_whole,
    quantity_whole_avg_2_prior_percent_rank,
    quantity_whole_bucket_percent_rank
FROM scored_inventory_2
ORDER BY first_known_quantity_whole_avg_3_prior DESC;
