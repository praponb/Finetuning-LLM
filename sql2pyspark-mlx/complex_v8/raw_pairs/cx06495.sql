WITH prepared AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        SUBSTRING(TRIM(zone), 1, 4) AS zone_prefix
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-05-10'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        zone,
        commissioned_on,
        RANK() OVER w_zone_by_commissioned_on AS commissioned_on_rank,
        NTILE(10) OVER w_zone_by_commissioned_on_2 AS commissioned_on_decile,
        ROW_NUMBER() OVER (PARTITION BY zone ORDER BY commissioned_on, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY commissioned_on DESC NULLS FIRST, id) AS commissioned_on_dense_rank,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER (PARTITION BY zone ORDER BY commissioned_on, id)) AS days_since_prev_commissioned_on,
        LAST_VALUE(commissioned_on) OVER (PARTITION BY zone ORDER BY commissioned_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_commissioned_on
    FROM prepared
    WINDOW w_zone_by_commissioned_on AS (PARTITION BY zone ORDER BY commissioned_on, id), w_zone_by_commissioned_on_2 AS (PARTITION BY zone ORDER BY commissioned_on DESC NULLS FIRST, id)
)
-- Subtotals with GROUPING SETS
SELECT
    rn,
    commissioned_on_dense_rank,
    GROUPING(rn) AS is_total_rn,
    SUM(CASE WHEN zone IS NOT NULL THEN rn ELSE 0 END) AS rn_zone_is_not_null,
    MAX(commissioned_on_decile) AS max_commissioned_on_decile,
    ROUND(AVG(commissioned_on_rank), 2) AS avg_commissioned_on_rank
FROM sequenced
GROUP BY GROUPING SETS ((rn, commissioned_on_dense_rank), (rn), (commissioned_on_dense_rank))
ORDER BY rn NULLS LAST, commissioned_on_dense_rank NULLS LAST
