WITH base AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE store_name LIKE '%Downtown%'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        SUM(sqft) OVER w_recent AS cumulative_sqft,
        SUM(sqft) OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date), region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM base
    WINDOW w_recent AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND MONTH(c.sale_date) IN (2, 7, 8)
        )
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM eligible s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 40
        )
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    clean_store_name,
    cumulative_sqft,
    running_sqft
FROM not_flagged
ORDER BY cumulative_sqft DESC NULLS FIRST;
