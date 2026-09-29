WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        YEAR(start_date) AS start_date_year
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.start_date_year,
        p.region AS holder_region
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score > 0.2
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(premium) AS max_premium
    FROM joined
    WHERE holder_id IS NOT NULL
    GROUP BY holder_id
    HAVING MAX(premium) > 1132.1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM stats)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(max_premium / NULLIF(distinct_ids, 0), 3) AS max_premium_per_distinct_ids,
        ROUND(distinct_ids / NULLIF(max_premium, 0), 3) AS distinct_ids_per_max_premium
    FROM filtered
    WHERE holder_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY distinct_ids)) AS distinct_holder_id_in_distinct_ids,
        RANK() OVER (PARTITION BY distinct_ids, holder_id ORDER BY distinct_ids_per_max_premium DESC) AS distinct_ids_per_max_premium_rank
    FROM enriched
)
SELECT
    holder_id,
    max_premium,
    max_premium_per_distinct_ids,
    ROUND(max_premium * 1.5, 2) AS max_premium_adj
FROM with_trends
