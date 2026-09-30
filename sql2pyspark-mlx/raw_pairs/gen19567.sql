WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST, id) AS row_num
    FROM stores
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        sqft,
        PERCENT_RANK() OVER (ORDER BY sqft DESC) sqft_percent_rank,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region,
        LEAD(sqft, 2, 0) OVER (PARTITION BY city ORDER BY opened_date, id) next_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) sqft_share
    FROM eligible
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft,
        s.sqft_share,
        x.inventory_quantity,
        x.last_restock
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        sqft,
        sqft_share,
        AVG(sqft) OVER (PARTITION BY city ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) sqft_avg_5_prior,
        AVG(sqft_share) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_share_avg_3_prior,
        RANK() OVER (ORDER BY inventory_quantity NULLS LAST) inventory_quantity_rank,
        SUM(sqft_share) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft_share,
        SUM(sqft) OVER (PARTITION BY city ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM with_latest
),
with_windows_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        sqft_share,
        running_sqft_share,
        SUM(sqft_avg_5_prior) OVER w_recent running_sum_sqft_avg_5_prior,
        AVG(sqft_avg_5_prior) OVER w_recent running_avg_sqft_avg_5_prior,
        COUNT(sqft_avg_5_prior) OVER w_recent running_count_sqft_avg_5_prior
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY city ORDER BY inventory_quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows_stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id = 1
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        running_sum_sqft_avg_5_prior,
        running_avg_sqft_avg_5_prior,
        sqft_share
    FROM eligible_stores
) src
UNPIVOT (
    val FOR metric_name IN (running_sum_sqft_avg_5_prior, running_avg_sqft_avg_5_prior, sqft_share)
)
