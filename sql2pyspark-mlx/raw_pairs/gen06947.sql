WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(host) AS host_lower
    FROM lines
    WHERE host NOT IN ('h4', 'h1')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        attr_key,
        value,
        host_lower,
        LENGTH(host) AS host_length,
        CASE WHEN service IN ('checkout', 'billing') THEN 'Group A' ELSE 'Other' END AS service_group,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM cleaned
    WHERE host = 'h3'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        value,
        host_lower,
        host_length,
        resolved_attr_key,
        COUNT(host_length) OVER w_ordered AS last3_count_host_length,
        AVG(host_length) OVER w_ordered AS last3_avg_host_length,
        MAX(host_length) OVER w_ordered AS last3_max_host_length
    FROM valid
    WHERE service <> 'search'
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY host_length ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        attr_key,
        host_lower,
        resolved_attr_key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY last3_avg_host_length ASC, id ASC, attr_key ASC) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    attr_key,
    host_lower,
    resolved_attr_key,
    CASE WHEN attr_key RLIKE '^(version|region)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(trace)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS attr_key_zone
FROM ranked
ORDER BY attr_key_zone ASC NULLS FIRST;
