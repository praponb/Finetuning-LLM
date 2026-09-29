WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.store_id IS NULL
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(region) IGNORE NULLS OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        LEAD(sqft, 1, 0) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS next_sqft,
        NTILE(10) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior
    FROM not_flagged
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY next_sqft DESC, id) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        PERCENT_RANK() OVER w_recent AS sqft_avg_2_prior_percent_rank,
        CUME_DIST() OVER (ORDER BY sqft_avg_2_prior DESC NULLS LAST) AS sqft_avg_2_prior_cume_dist,
        NTILE(5) OVER (PARTITION BY city ORDER BY next_sqft DESC, id) AS next_sqft_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM deduped
    WINDOW w_ordered AS (ORDER BY sqft, id), w_recent AS (ORDER BY sqft_avg_2_prior DESC NULLS FIRST)
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        CASE WHEN sqft >= 16939 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
        COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count,
        BIT_OR(sqft) AS sqft_bit_or
    FROM enriched
    GROUP BY DATE_TRUNC('QUARTER', opened_date), CASE WHEN sqft >= 16939 THEN 'HIGH' ELSE 'LOW' END
    HAVING sqft_bit_or < 1
)
SELECT quarter_start, sqft_tier
FROM grouped
ORDER BY quarter_start DESC, sqft_tier DESC
