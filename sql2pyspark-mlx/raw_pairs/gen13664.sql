WITH filtered AS (
    SELECT *, CASE WHEN sqft >= 12330 THEN 'High' WHEN sqft >= 4278 THEN 'Medium' ELSE 'Low' END AS sqft_tier
    FROM stores
    WHERE region IS NOT NULL
      AND region <> 'West'
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date
        )
),
metrics AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        sqft_tier,
        AVG(sqft) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date), region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
        NTILE(10) OVER w_running AS sqft_bucket
    FROM eligible
    WINDOW w_running AS (PARTITION BY region ORDER BY sqft DESC, id)
),
matched_keys AS (
    SELECT region
    FROM metrics
    INTERSECT ALL
    SELECT region
    FROM stores
    WHERE city IN ('New York', 'London')
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.region = k.region
),
with_windows AS (
    SELECT
        id,
        store_name,
        region,
        sqft,
        sqft_avg_6_prior,
        sqft_bucket,
        CUME_DIST() OVER w_hist AS sqft_avg_6_prior_cume_dist,
        DENSE_RANK() OVER (ORDER BY sqft_bucket DESC) AS sqft_bucket_dense_rank
    FROM reconciled
    WINDOW w_hist AS (ORDER BY sqft_avg_6_prior DESC NULLS LAST)
)
SELECT
    id,
    sqft,
    sqft_avg_6_prior,
    sqft_bucket
FROM with_windows;
