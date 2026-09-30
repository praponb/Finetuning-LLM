WITH cleaned AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE product IN ('HOME', 'AUTO', 'HEALTH')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region = 'South'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY holder_region, status)) AS distinct_holder_id_in_holder_region_status,
        RANK() OVER (PARTITION BY status ORDER BY holder_joined_on, id) AS holder_joined_on_rank,
        SUM(premium) OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_sum_7,
        MIN(premium) OVER (PARTITION BY holder_region, status) AS holder_region_status_min_premium,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        LEAD(end_date, 2) OVER (ORDER BY premium NULLS LAST, id) AS next2_end_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        NTILE(3) OVER w_all_by_premium AS premium_tercile,
        LAG(start_date) OVER (PARTITION BY status, holder_region ORDER BY holder_joined_on DESC NULLS FIRST, id) AS prev_start_date
    FROM with_parent
    WINDOW w_all_by_premium AS (ORDER BY premium NULLS LAST, id)
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    holder_joined_on,
    distinct_holder_id_in_holder_region_status,
    premium_sum_7,
    holder_region_status_min_premium,
    first_known_start_date,
    premium_tercile
FROM ranked
ORDER BY premium_sum_7
