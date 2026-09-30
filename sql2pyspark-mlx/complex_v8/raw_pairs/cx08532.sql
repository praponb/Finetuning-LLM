WITH prepared_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        CASE WHEN rate >= 6.1 THEN 'large' WHEN rate >= 4.2 THEN 'medium' ELSE 'small' END AS rate_tier
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-09 05:00:00'
      AND tariff NOT IN ('FLAT', 'EV')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate_tier,
        ROW_NUMBER() OVER (PARTITION BY YEAR(valid_to) ORDER BY valid_from DESC NULLS FIRST, id) AS rn,
        LEAD(valid_to) OVER (PARTITION BY YEAR(valid_from) ORDER BY valid_to DESC, id) AS next_valid_to,
        ROUND(PERCENT_RANK() OVER w_valid_from_year_by_valid_to_2, 3) AS valid_to_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(valid_from) ORDER BY valid_to DESC), 3) AS valid_to_cume_dist,
        LEAD(valid_from) OVER (PARTITION BY YEAR(valid_to) ORDER BY valid_from DESC NULLS FIRST, id) AS next_valid_from,
        ROUND(CUME_DIST() OVER w_valid_from_year_by_valid_to_3, 3) AS valid_to_cume_dist_2,
        LAST_VALUE(valid_to) OVER (PARTITION BY YEAR(valid_from) ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_to,
        LAG(valid_from) OVER (PARTITION BY YEAR(valid_to) ORDER BY valid_from DESC NULLS FIRST, id) AS prev_valid_from
    FROM prepared_tariffs
    WINDOW w_valid_from_year_by_valid_to_2 AS (PARTITION BY YEAR(valid_from) ORDER BY valid_to DESC), w_valid_from_year_by_valid_to_3 AS (PARTITION BY YEAR(valid_from) ORDER BY valid_to ASC)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate_tier,
        next_valid_to,
        valid_to_percent_rank,
        valid_to_cume_dist,
        next_valid_from,
        valid_to_cume_dist_2,
        TO_DATE(prev_valid_from) AS prev_valid_from_date,
        CASE WHEN rn >= 8 THEN 'A' WHEN rn >= 3 THEN 'B' ELSE 'C' END AS rn_tier
    FROM metrics
)
SELECT
    id,
    tariff,
    valid_from,
    rate_tier,
    next_valid_to,
    valid_to_percent_rank,
    valid_to_cume_dist,
    next_valid_from,
    prev_valid_from_date,
    ROUND(valid_to_percent_rank / NULLIF(valid_to_cume_dist, 0), 3) AS valid_to_percent_rank_per_valid_to_cume_dist
FROM annotated
WHERE valid_to_cume_dist_2 <= 0.6
ORDER BY valid_from
