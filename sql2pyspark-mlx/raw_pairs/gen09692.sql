WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC NULLS FIRST, id) AS row_num
    FROM sessions
    QUALIFY row_num = 1
),
deduped_sessions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        row_num
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY row_num DESC, id) AS latest_rank
        FROM deduped
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, c.age
    FROM deduped_sessions s
    JOIN customers c
        ON s.customer_id = c.id
    WHERE UPPER(c.name) LIKE '%MMA%'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(age AS STRING) age_str,
        ROUND(age / row_num, 2) age_per_row_num,
        COALESCE(country, 'NONE') country_clean
    FROM enriched
    WHERE started_at > DATE '2023-05-10'
      AND country = 'DE'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.segment
    FROM valid s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN age_per_row_num >= 60 THEN 'LARGE' ELSE 'SMALL' END AS age_per_row_num_tier,
        DATE_TRUNC('MONTH', started_at) AS activity_month,
        ROUND(AVG(age_per_row_num), 2) AS avg_age_per_row_num,
        ROUND(SUM(age_per_row_num) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS age_per_row_num_per_smb,
        ROUND(AVG(age_per_row_num), 2) AS avg_age_per_row_num_2,
        SIZE(COLLECT_SET(browser)) AS num_browsers,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        GROUPING(CASE WHEN age_per_row_num >= 60 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM with_dims
    GROUP BY CUBE (CASE WHEN age_per_row_num >= 60 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', started_at))
)
SELECT
    age_per_row_num_tier,
    activity_month,
    age_per_row_num_per_smb,
    avg_age_per_row_num_2,
    num_browsers,
    country_list,
    num_segments,
    gid
FROM stats;
