WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(location, 1, 4) AS location_prefix
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location = 'Singapore'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        location,
        budget,
        location_prefix,
        CONCAT(dept_name, ' ', location) AS dept_name_location,
        INITCAP(TRIM(location)) AS location_title
    FROM valid
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS seq_no,
        FIRST_VALUE(location, TRUE) OVER w_part AS first_known_location,
        LAST_VALUE(location, TRUE) OVER w_part AS last_known_location
    FROM filtered
    WINDOW w_part AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SIZE(COLLECT_SET(location)) AS num_locations
FROM with_windows
GROUP BY seq_no
ORDER BY seq_no DESC;
