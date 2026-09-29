WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity * 1.07, 2) AS quantity_scaled
    FROM inventory
    WHERE last_restock <= '2023-10-01'
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.cost
    FROM base src
    JOIN products p
        ON src.product_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS FIRST, id ASC) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.category AS product_category,
        d.product_name
    FROM latest a
    JOIN products d
        ON a.product_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id ASC) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(dedup_rank) AS dedup_rank_whole
    FROM ranked
    WHERE last_restock > DATE '2024-04-26'
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            product_id,
            product_category,
            dedup_rank_whole
        FROM filtered
    ) src
    PIVOT (
        MAX(dedup_rank_whole)
        FOR product_category IN ('Home' AS home, 'Sports' AS sports)
    )
)
SELECT
    product_id,
    home,
    sports
FROM pivoted
ORDER BY product_id DESC
LIMIT 5;
