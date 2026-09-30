WITH valid AS (
    SELECT
        *,
        CONCAT(dept_name, ' - ', location) AS dept_name_location,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rank_in_group
    FROM valid
) t
WHERE rank_in_group = 1;
