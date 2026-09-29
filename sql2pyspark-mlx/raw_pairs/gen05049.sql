WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, v
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY created_at DESC, id ASC, attr_key ASC) = 1
),
latest_carts AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        attr_key,
        v
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY created_at DESC, id ASC, attr_key ASC) AS rank_in_group
        FROM latest
    ) t
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        TRIM(attr_key) AS clean_attr_key
    FROM latest_carts
    WHERE attr_key = 'utm_source'
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(attr_key) AS attr_key_upper
    FROM cleaned
    WHERE attr_key IS NOT DISTINCT FROM 'gift'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_key,
        attr_key_title,
        attr_key_upper,
        UPPER(attr_key) AS attr_key_upper_2,
        CASE WHEN attr_key = 'utm_source' THEN 1 ELSE 0 END AS is_utm_source,
        COALESCE(attr_key, 'NONE') AS resolved_attr_key
    FROM cleaned_carts
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        attr_key_title,
        attr_key_upper,
        attr_key_upper_2,
        resolved_attr_key,
        MIN(is_utm_source) OVER w_running AS running_min_is_utm_source,
        SUM(is_utm_source) OVER w_running AS running_sum_is_utm_source,
        MAX(is_utm_source) OVER w_running AS running_max_is_utm_source,
        AVG(is_utm_source) OVER w_running AS running_avg_is_utm_source,
        LEAD(is_utm_source, 1) OVER w_grp AS next_is_utm_source
    FROM filtered
    WHERE is_utm_source > 67
    WINDOW w_running AS (PARTITION BY attr_key ORDER BY is_utm_source ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY attr_key ORDER BY is_utm_source ASC, id ASC, attr_key ASC)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_min_is_utm_source >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_is_utm_source_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT_IF(attr_key = 'device') AS device_events,
        ROUND(SUM(running_avg_is_utm_source) / NULLIF(COUNT(CASE WHEN attr_key = 'weight_kg' THEN 1 END), 0), 2) AS running_avg_is_utm_source_per_weight_kg,
        MAX(running_avg_is_utm_source) AS max_running_avg_is_utm_source
    FROM with_windows
    GROUP BY CASE WHEN running_min_is_utm_source >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING device_events >= 1
)
SELECT
    running_min_is_utm_source_tier,
    device_events,
    running_avg_is_utm_source_per_weight_kg,
    max_running_avg_is_utm_source
FROM stats
ORDER BY running_min_is_utm_source_tier DESC
