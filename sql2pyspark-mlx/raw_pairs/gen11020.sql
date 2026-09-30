WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS dedup_rank
        FROM promotions
    ) x
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct >= 0.34 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    COUNT(*) AS row_count,
    AVG(discount_pct) AS avg_discount_pct,
    GROUPING(CASE WHEN discount_pct >= 0.34 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM deduped
GROUP BY CUBE (CASE WHEN discount_pct >= 0.34 THEN 'HIGH' ELSE 'LOW' END)
HAVING AVG(discount_pct) >= 0
ORDER BY row_count DESC;
