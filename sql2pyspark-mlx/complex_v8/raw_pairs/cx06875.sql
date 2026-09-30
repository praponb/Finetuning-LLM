WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        TO_DATE(closed_at) AS closed_at_date,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref
    FROM tickets
    WHERE (csat < 5 OR channel = 'chat')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', closed_at_date) AS closed_at_date_month,
        COUNT(*) AS n_rows,
        MIN(closed_at_date) AS first_closed_at_date,
        SUM(csat) AS total_csat,
        COUNT(DISTINCT priority) AS distinct_prioritys,
        MIN(csat) AS min_csat
    FROM cleaned
    GROUP BY 1
),
annotated AS (
    -- Add calculated columns
    SELECT *, COALESCE(min_csat, 0) AS min_csat_filled
    FROM per_group
    WHERE min_csat <= 3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY min_csat_filled, min_csat ORDER BY total_csat ASC) AS total_csat_rank,
        MAX(min_csat_filled) OVER (PARTITION BY YEAR(first_closed_at_date)) AS first_closed_at_date_year_max_min_csat_filled,
        MIN(min_csat_filled) OVER w_first_closed_at_date_year AS first_closed_at_date_year_min_min_csat_filled
    FROM annotated
    WINDOW w_first_closed_at_date_year AS (PARTITION BY YEAR(first_closed_at_date))
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE first_closed_at_date_year_min_min_csat_filled > (SELECT AVG(first_closed_at_date_year_min_min_csat_filled) FROM sequenced)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM screened
    WHERE total_csat_rank <= 3
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        closed_at_date_month,
        first_closed_at_date,
        total_csat,
        min_csat,
        min_csat_filled,
        first_closed_at_date_year_max_min_csat_filled,
        first_closed_at_date_year_min_min_csat_filled,
        MAX(total_csat_rank) OVER (PARTITION BY total_csat_rank ORDER BY n_rows NULLS LAST, closed_at_date_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_csat_rank_max_4,
        ROW_NUMBER() OVER (PARTITION BY n_rows ORDER BY min_csat ASC, closed_at_date_month) AS min_csat_seq,
        COUNT(*) OVER w_distinct_prioritys_min_csat AS rows_in_distinct_prioritys_min_csat_filled,
        NTH_VALUE(min_csat, 2) OVER (PARTITION BY total_csat_rank ORDER BY n_rows NULLS LAST, closed_at_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_csat,
        ROUND(total_csat_rank / NULLIF(SUM(total_csat_rank) OVER (PARTITION BY distinct_prioritys, min_csat_filled), 0), 4) AS total_csat_rank_share_of_distinct_prioritys_min_
    FROM first_rows
    WINDOW w_distinct_prioritys_min_csat AS (PARTITION BY distinct_prioritys, min_csat_filled)
)
SELECT
    closed_at_date_month,
    first_closed_at_date,
    total_csat,
    min_csat,
    min_csat_filled,
    first_closed_at_date_year_max_min_csat_filled,
    first_closed_at_date_year_min_min_csat_filled,
    min_csat_seq,
    rows_in_distinct_prioritys_min_csat_filled,
    second_min_csat,
    total_csat_rank_share_of_distinct_prioritys_min_,
    COALESCE(total_csat, 0) AS total_csat_filled,
    ABS(total_csat_rank_max_4 - 21) AS total_csat_rank_max_4_distance
FROM metrics
ORDER BY min_csat DESC, rows_in_distinct_prioritys_min_csat_filled NULLS LAST
