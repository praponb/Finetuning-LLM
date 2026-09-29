WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        DATE_ADD(start_date, 30) AS start_date_plus_30d
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED')
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE UPPER(TRIM(p.holder_name)) LIKE 'G%'
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM joined_holder s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.start_date IS NOT NULL
              AND p.status NOT IN ('CANCELLED', 'LAPSED')
        )
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM filtered
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM filtered s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
)
SELECT
    status,
    product,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT id) AS distinct_ids,
    DATEDIFF(MAX(start_date_plus_30d), MIN(start_date_plus_30d)) AS start_date_plus_30d_span_days
FROM above_average
GROUP BY CUBE(status, product)
ORDER BY status NULLS LAST, product NULLS LAST
