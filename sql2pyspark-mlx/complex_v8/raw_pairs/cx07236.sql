WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE status <> 'LAPSED'
      AND end_date >= DATE '2025-09-27'
),
product_stats AS (
    -- Avg premium_adj per product
    SELECT product, AVG(premium_adj) AS avg_premium_adj
    FROM recent_policys
    GROUP BY product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_adj,
        ROUND(s.premium_adj - g.avg_premium_adj, 2) AS premium_adj_minus_avg
    FROM recent_policys s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium_adj > g.avg_premium_adj
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        status,
        status_slug,
        status_or_unknown,
        avg_premium_adj,
        COUNT(*) OVER w_holder_id_status_or_unknown AS rows_in_holder_id_status_or_unknown,
        LAG(avg_premium_adj, 3) OVER w_product_holder_id_by_premium AS prev3_avg_premium_adj
    FROM compared
    WINDOW w_holder_id_status_or_unknown AS (PARTITION BY holder_id, status_or_unknown), w_product_holder_id_by_premium AS (PARTITION BY product, holder_id ORDER BY premium_adj DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        status_or_unknown,
        holder_id,
        MAX(prev3_avg_premium_adj) AS max_prev3_avg_premium_adj,
        COUNT(*) FILTER (WHERE status = 'CANCELLED') AS n_status_cancelled,
        ROUND(AVG(rows_in_holder_id_status_or_unknown), 2) AS avg_rows_in_holder_id_status_or_unknown,
        COUNT(*) AS cnt,
        ROUND(STDDEV(avg_premium_adj), 2) AS stddev_avg_premium_adj
    FROM with_trends
    WHERE status_or_unknown IS NOT NULL
    GROUP BY status_or_unknown, holder_id
),
with_parent AS (
    -- Attach holder attributes
    SELECT s.*, p.segment AS holder_segment, p.joined_on AS holder_joined_on
    FROM totals s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IS NOT NULL
)
SELECT
    status_or_unknown,
    n_status_cancelled,
    holder_id,
    ROUND(SUM(avg_rows_in_holder_id_status_or_unknown) / NULLIF(SUM(max_prev3_avg_premium_adj), 0), 3) AS avg_rows_in_holder_id_status_or_unknown_to_max_p,
    ROUND(AVG(avg_rows_in_holder_id_status_or_unknown), 2) AS avg_avg_rows_in_holder_id_status_or_unknown
FROM with_parent
GROUP BY GROUPING SETS ((status_or_unknown, n_status_cancelled, holder_id), (status_or_unknown), ())
ORDER BY status_or_unknown NULLS LAST, n_status_cancelled NULLS LAST, holder_id NULLS LAST
