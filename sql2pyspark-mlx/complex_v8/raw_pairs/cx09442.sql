WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        region,
        joined_on,
        segment
    FROM policyholders
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MAX(start_date) AS last_policy_start_date
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.last_policy_start_date
    FROM base s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        region,
        segment,
        last_policy_start_date,
        ADD_MONTHS(last_policy_start_date, 3) AS last_policy_start_date_plus_3m,
        CASE WHEN region IN ('South', 'East') THEN 'group_a' WHEN region = 'North' THEN 'group_b' ELSE 'other' END AS region_group
    FROM with_counts
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.product = 'HOME'
              AND p.status NOT IN ('CANCELLED', 'ACTIVE')
        )
      AND s.segment NOT IN ('VIP', 'SME')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        region,
        segment,
        last_policy_start_date,
        last_policy_start_date_plus_3m,
        region_group,
        NTH_VALUE(last_policy_start_date, 2) OVER (PARTITION BY region_group ORDER BY last_policy_start_date_plus_3m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_policy_start_date,
        LEAD(last_policy_start_date, 3) OVER (PARTITION BY region_group ORDER BY last_policy_start_date_plus_3m DESC, id) AS next3_last_policy_start_date,
        LAG(last_policy_start_date) OVER (PARTITION BY region_group ORDER BY last_policy_start_date DESC, id) AS prev_last_policy_start_date,
        LEAD(last_policy_start_date, 3) OVER (PARTITION BY region_group ORDER BY last_policy_start_date DESC, id) AS next3_last_policy_start_date_2,
        ROW_NUMBER() OVER w_region_group_by_last_policy_3 AS rn,
        LAG(last_policy_start_date) OVER (PARTITION BY region_group ORDER BY last_policy_start_date_plus_3m DESC, id) AS prev_last_policy_start_date_2
    FROM filtered
    WINDOW w_region_group_by_last_policy_3 AS (PARTITION BY region_group ORDER BY last_policy_start_date DESC, id)
),
line_level_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.region,
        s.segment,
        s.last_policy_start_date,
        s.last_policy_start_date_plus_3m,
        s.second_last_policy_start_date,
        s.next3_last_policy_start_date,
        s.prev_last_policy_start_date,
        s.next3_last_policy_start_date_2,
        s.rn,
        s.prev_last_policy_start_date_2,
        p.id AS policy_id,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM with_trends s
    JOIN policies p ON p.holder_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_end_date, next3_last_policy_start_date, segment, rn
    FROM line_level_policys
) src
PIVOT (
    COUNT(rn)
    FOR segment IN ('RETAIL', 'VIP', 'SME')
)
ORDER BY policy_end_date, next3_last_policy_start_date
