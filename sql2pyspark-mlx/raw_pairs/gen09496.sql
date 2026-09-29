WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REPLACE(store_name, ' ', '-') AS store_name_slug,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE MONTH(opened_date) IN (4, 8, 10)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        region,
        opened_date,
        sqft,
        region_filled,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft,
        COUNT(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        MAX(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        MIN(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft NULLS LAST) AS sqft_cume_dist
    FROM base
    WHERE opened_date > '2024-05-21'
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        sqft,
        region_filled,
        running_sum_sqft,
        running_count_sqft,
        sqft_cume_dist,
        SUM(sqft_cume_dist) OVER (PARTITION BY region ORDER BY running_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft_cume_dist,
        MIN(sqft_cume_dist) OVER (PARTITION BY region ORDER BY running_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft_cume_dist,
        COUNT(sqft_cume_dist) OVER (PARTITION BY region ORDER BY running_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft_cume_dist,
        AVG(sqft_cume_dist) OVER (PARTITION BY region ORDER BY running_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft_cume_dist,
        RANK() OVER (PARTITION BY region ORDER BY running_min_sqft DESC NULLS LAST) AS running_min_sqft_rank
    FROM latest_match
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date
        )
),
report AS (
    SELECT
        id,
        opened_date,
        sqft,
        region_filled,
        running_count_sqft,
        running_sum_sqft_cume_dist,
        running_min_sqft_cume_dist
    FROM not_flagged
    ORDER BY id DESC
)
SELECT
    id,
    opened_date,
    sqft,
    region_filled,
    running_count_sqft,
    running_sum_sqft_cume_dist,
    running_min_sqft_cume_dist
FROM report
ORDER BY running_count_sqft DESC NULLS FIRST, running_min_sqft_cume_dist DESC
