WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        holder_id,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        ROUND(100 * COUNT(CASE WHEN status IN ('EXPIRED', 'ACTIVE', 'LAPSED') THEN 1 END) / COUNT(*), 1) AS pct_status_in_expired_active_lap,
        MIN(premium) AS min_premium,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        SUM(CASE WHEN status <> 'ACTIVE' THEN 1 ELSE 0 END) AS n_status_active,
        SUM(premium) FILTER (WHERE status IN ('LAPSED', 'EXPIRED')) AS premium_status_in_lapsed_expired
    FROM scoped_policys
    WHERE start_date >= DATE '2024-08-27'
    GROUP BY status, holder_id, DATE_FORMAT(start_date, 'yyyy-MM')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        status,
        holder_id,
        start_date_ym,
        min_premium,
        stddev_premium,
        LAST_VALUE(n_status_active) OVER (PARTITION BY n_status_active, status ORDER BY stddev_premium, status, holder_id, start_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n_status_active,
        ROUND(CUME_DIST() OVER w_n_status_active_status_by_pr, 3) AS premium_status_in_lapsed_expired_cume_dist
    FROM per_group
    WINDOW w_n_status_active_status_by_pr AS (PARTITION BY n_status_active, status ORDER BY premium_status_in_lapsed_expired)
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.status,
        s.holder_id,
        s.start_date_ym,
        s.stddev_premium,
        s.last_n_status_active,
        s.premium_status_in_lapsed_expired_cume_dist,
        p.joined_on AS holder_joined_on
    FROM sequenced s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on >= DATE '2024-09-04'
),
enriched AS (
    -- Join policyholders
    SELECT
        s.status,
        s.holder_id,
        s.start_date_ym,
        s.stddev_premium,
        s.holder_joined_on,
        p.segment AS holder_segment
    FROM combined_details s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    status,
    holder_id,
    start_date_ym,
    stddev_premium,
    holder_joined_on,
    holder_segment
FROM enriched
ORDER BY holder_segment DESC
