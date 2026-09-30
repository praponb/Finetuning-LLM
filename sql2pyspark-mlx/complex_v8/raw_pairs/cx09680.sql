WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT_WS('|', status, product) AS status_product_label,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE status NOT IN ('ACTIVE', 'CANCELLED')
      AND product = 'HOME'
),
per_group AS (
    -- Group-level totals
    SELECT
        status,
        MAX(premium) AS max_premium,
        SUM(status_len) AS total_status_len,
        ROUND(100 * COUNT(CASE WHEN status <> 'EXPIRED' THEN 1 END) / COUNT(*), 1) AS pct_status_expired
    FROM scoped_policys
    GROUP BY 1
    HAVING MAX(premium) > 2111.3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        status,
        max_premium,
        total_status_len,
        pct_status_expired,
        FIRST_VALUE(pct_status_expired) IGNORE NULLS OVER (PARTITION BY status ORDER BY max_premium DESC, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pct_status_expired,
        FIRST_VALUE(max_premium) IGNORE NULLS OVER (PARTITION BY status ORDER BY total_status_len DESC, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_premium,
        total_status_len - LAG(total_status_len) OVER w_status_by_total_status_len AS total_status_len_change,
        MIN(pct_status_expired) OVER (PARTITION BY status ORDER BY total_status_len DESC, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_pct_status_expired_so_far,
        NTH_VALUE(pct_status_expired, 2) OVER (PARTITION BY status ORDER BY max_premium DESC, status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_status_expired
    FROM per_group
    WINDOW w_status_by_total_status_len AS (PARTITION BY status ORDER BY total_status_len DESC, status)
),
annotated AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(status)) AS status_len
    FROM windowed
    WHERE first_known_pct_status_expired >= 10.0
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM annotated
    WHERE first_known_pct_status_expired < (SELECT MAX(first_known_pct_status_expired) FROM annotated)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE total_status_len > (SELECT AVG(total_status_len) FROM screened)
)
-- Final projection
SELECT
    status,
    max_premium,
    total_status_len,
    pct_status_expired,
    min_pct_status_expired_so_far,
    second_pct_status_expired,
    status_len,
    SUBSTRING(TRIM(status), 1, 2) AS status_prefix
FROM without_matches
ORDER BY status_len DESC, status_prefix, status
LIMIT 5
