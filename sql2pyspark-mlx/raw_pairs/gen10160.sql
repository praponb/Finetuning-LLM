WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CASE WHEN region IN ('South', 'North') THEN 'Priority' ELSE 'Other' END AS region_group,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND store_name LIKE '%Airpo%'
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft,
        latest.status
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft NULLS FIRST) AS sqft_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY city, status ORDER BY sqft DESC, id) AS seq_no
    FROM with_latest
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        status,
        sqft_dense_rank,
        AVG(seq_no) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS seq_no_avg_5_prior,
        NTILE(10) OVER (PARTITION BY city ORDER BY sqft_dense_rank, id) AS sqft_dense_rank_bucket,
        MIN(sqft_dense_rank) OVER (PARTITION BY status, city) AS status_min_sqft_dense_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, status), 4) AS sqft_share
    FROM metrics
)
SELECT
    id,
    store_name,
    city,
    sqft_dense_rank,
    seq_no_avg_5_prior,
    sqft_dense_rank_bucket,
    status_min_sqft_dense_rank,
    sqft_share,
    CASE WHEN city RLIKE '^(Chiang Mai|London)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'CORE' ELSE 'GROUP_A' END AS city_zone
FROM with_windows;
