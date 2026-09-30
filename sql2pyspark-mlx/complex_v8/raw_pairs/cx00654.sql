WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        CASE WHEN product IN ('LIFE', 'HOME') THEN 'group_a' WHEN product = 'HEALTH' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE (status IN ('CANCELLED', 'LAPSED') OR product = 'AUTO')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(days_end_date_to_start_date - AVG(days_end_date_to_start_date) OVER w_start_date_year, 2) AS days_end_date_to_start_date_vs_start_date_year_a,
        LAG(days_end_date_to_start_date, 3) OVER (PARTITION BY YEAR(start_date) ORDER BY days_end_date_to_start_date, id) AS prev3_days_end_date_to_start_date,
        SUM(days_end_date_to_start_date) OVER (PARTITION BY YEAR(start_date) ORDER BY days_end_date_to_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_end_date_to_start_date,
        ROUND(CUME_DIST() OVER w_start_date_year_by_days_end_3, 3) AS days_end_date_to_start_date_cume_dist,
        FIRST_VALUE(end_date) OVER (PARTITION BY YEAR(start_date) ORDER BY days_end_date_to_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM valid_policys
    WINDOW w_start_date_year AS (PARTITION BY YEAR(start_date)), w_start_date_year_by_days_end_3 AS (PARTITION BY YEAR(start_date) ORDER BY days_end_date_to_start_date)
),
latest AS (
    -- Keep the earliest row per holder_id, product_group
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        days_end_date_to_start_date,
        product_group,
        days_end_date_to_start_date_vs_start_date_year_a,
        prev3_days_end_date_to_start_date,
        running_days_end_date_to_start_date,
        days_end_date_to_start_date_cume_dist,
        first_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product_group ORDER BY days_end_date_to_start_date_vs_start_date_year_a, id) AS row_num
        FROM windowed
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT first_end_date, product_group, prev3_days_end_date_to_start_date
    FROM latest
) src
PIVOT (
    SUM(prev3_days_end_date_to_start_date)
    FOR product_group IN ('group_a', 'other', 'group_b')
)
