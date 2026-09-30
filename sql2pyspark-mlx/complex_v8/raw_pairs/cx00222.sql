WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE status = 'EXPIRED'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(premium) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC), 3) AS start_date_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC), 3) AS start_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC, id), 3) AS start_date_percent_rank_2,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC, id) AS rn,
        SUM(premium) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior
    FROM cleaned_policys
)
SELECT
    holder_id,
    status,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM with_windows
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), (status))
ORDER BY holder_id NULLS LAST, status NULLS LAST
