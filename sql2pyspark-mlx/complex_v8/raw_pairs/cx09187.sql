WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_ADD(end_date, 90) AS end_date_plus_90d
    FROM policies
    WHERE (premium >= 1042.8 OR status NOT IN ('ACTIVE', 'CANCELLED'))
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.status_group,
        s.end_date_plus_90d,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(holder_joined_on) AS holder_joined_on_year,
        COUNT(*) AS row_count,
        COUNT(DISTINCT status) AS distinct_status,
        ARRAY_SORT(COLLECT_SET(holder_region)) AS holder_region_list,
        SUM(CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END) AS n_status_is_not_null,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM combined_details_holder
    GROUP BY YEAR(holder_joined_on)
    HAVING COUNT(*) >= 2
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE holder_joined_on_year > (SELECT AVG(holder_joined_on_year) FROM per_group)
)
SELECT
    holder_joined_on_year,
    row_count,
    distinct_status,
    holder_region_list,
    n_status_is_not_null
FROM filtered
WHERE row_count > 4
ORDER BY row_count DESC NULLS FIRST, distinct_status
