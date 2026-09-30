WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.units > 20
        )
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.channel,
        x.status
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        sqft,
        channel,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        LAG(sqft, 1, 0) OVER w_running AS prev_sqft,
        SUM(sqft) OVER (PARTITION BY channel, region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM latest_match
    WINDOW w_running AS (PARTITION BY status ORDER BY opened_date ASC, id ASC)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY sqft ASC NULLS LAST, id ASC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY prev_sqft ASC, id ASC) AS next_sqft,
        LAG(sqft) OVER (PARTITION BY region ORDER BY prev_sqft ASC, id ASC) AS prev_sqft_2,
        LEAD(prev_sqft) OVER (PARTITION BY region ORDER BY prev_sqft ASC, id ASC) AS next_prev_sqft,
        LAG(prev_sqft) OVER (PARTITION BY region ORDER BY prev_sqft ASC, id ASC) AS prev_prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY prev_sqft ASC, id ASC) AS seq_no
    FROM ranked
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_sqft >= 16675 THEN 'HIGH' ELSE 'LOW' END AS prev_sqft_tier,
    CASE WHEN next_prev_sqft >= 12307 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_prev_sqft_tier,
    SUM(prev_sqft_2) AS total_prev_sqft_2,
    SUM(CASE WHEN channel = 'partner' THEN seq_no ELSE 0 END) AS partner_seq_no
FROM enriched
GROUP BY CASE WHEN prev_sqft >= 16675 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_prev_sqft >= 12307 THEN 'PREMIUM' ELSE 'STANDARD' END;
