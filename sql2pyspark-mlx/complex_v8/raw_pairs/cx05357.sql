WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
),
holder_id_stats AS (
    -- Avg premium per holder_id
    SELECT holder_id, AVG(premium) AS avg_premium
    FROM base
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM base s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium > g.avg_premium
),
latest_per_key AS (
    -- Keep the latest row per status, holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        status_prefix,
        avg_premium,
        premium_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY avg_premium DESC, id DESC) AS recency_rank
        FROM vs_group
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, premium_minus_avg, avg_premium, premium
    FROM latest_per_key
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (premium_minus_avg AS m_premium_minus_avg, avg_premium AS m_avg_premium, premium AS m_premium)
) unp
ORDER BY id, metric_name
