WITH filtered_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_label
    FROM policyholders
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        joined_on_label,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
    FROM filtered_holders
    WHERE joined_on >= DATE '2024-06-29'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_name,
        joined_on,
        segment,
        joined_on_label,
        holder_name_slug,
        ROUND(PERCENT_RANK() OVER (PARTITION BY region, segment ORDER BY joined_on, id), 3) AS joined_on_percent_rank,
        LAG(joined_on, 2) OVER (PARTITION BY region, segment ORDER BY joined_on, id) AS prev2_joined_on,
        LAG(joined_on, 3) OVER w_all_by_joined_on AS prev3_joined_on,
        LAG(joined_on) OVER (PARTITION BY region, segment ORDER BY joined_on, id) AS prev_joined_on,
        LAST_VALUE(joined_on) OVER (ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_joined_on
    FROM enriched
    WINDOW w_all_by_joined_on AS (ORDER BY joined_on ASC, id)
)
SELECT
    id,
    holder_name,
    joined_on,
    segment,
    joined_on_label,
    holder_name_slug,
    joined_on_percent_rank,
    prev2_joined_on,
    prev3_joined_on,
    last_joined_on
FROM ranked
ORDER BY prev3_joined_on, id
LIMIT 10
