WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN sqft >= 17422 THEN 'High' WHEN sqft >= 4942 THEN 'Medium' ELSE 'Low' END AS sqft_level,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM stores
    WHERE opened_date >= DATE '2023-03-26'
      AND region IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY sqft NULLS FIRST) AS sqft_rank,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY sqft, id) AS sqft_bucket,
        CUME_DIST() OVER (ORDER BY sqft DESC NULLS LAST) AS sqft_cume_dist
    FROM filtered
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft_bucket DESC, id) = 1
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.units,
        top1.store_sale_revenue
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(sqft_rank, 1) OVER (PARTITION BY city ORDER BY sqft_bucket, id) AS next_sqft_rank,
        SUM(sqft_rank) OVER w_hist AS running_sqft_rank,
        COUNT(sqft) OVER (PARTITION BY city) AS city_count_sqft,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS event_seq
    FROM with_latest
    WINDOW w_hist AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked_stores AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft_bucket) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM ranked_stores
    EXCEPT
    SELECT city, id
    FROM stores
    WHERE LENGTH(store_name) < 5
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_stores s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    sqft_rank,
    sqft_cume_dist,
    units,
    next_sqft_rank,
    CASE WHEN region RLIKE '^(East|Central)$' THEN 'CORE' WHEN region RLIKE '^(North|South)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS region_zone
FROM kept_rows
ORDER BY next_sqft_rank DESC NULLS LAST;
