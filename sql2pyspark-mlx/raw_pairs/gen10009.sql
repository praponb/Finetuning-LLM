WITH valid AS (
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(dept_name, 'OTHER') AS dept_name_clean
    FROM departments
    WHERE budget <= 3102000
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id ASC) AS dedup_rank
    FROM valid
) x
WHERE dedup_rank = 1
