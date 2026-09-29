WITH valid_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        LAG(rate) OVER w_all_by_valid_to AS prev_rate,
        LAST_VALUE(rate) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate,
        MAX(rate) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rate_max_2,
        LEAD(valid_to, 2) OVER (ORDER BY valid_to DESC, id) AS next2_valid_to
    FROM valid_tariffs
    WINDOW w_all_by_valid_to AS (ORDER BY valid_to DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        valid_to,
        rate,
        last_rate,
        next2_valid_to,
        DATE_FORMAT(next2_valid_to, 'MMM yyyy') AS next2_valid_to_label,
        TO_DATE(next2_valid_to) AS next2_valid_to_date
    FROM with_trends
    WHERE rate_max_2 > 10.8
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(next2_valid_to_date, 'yyyy-MM') AS next2_valid_to_date_ym,
        MIN(valid_to) AS first_valid_to,
        MAX(next2_valid_to) AS last_next2_valid_to,
        SUM(rate) AS total_rate,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM derived
    GROUP BY 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(first_valid_to) OVER (ORDER BY cnt DESC, next2_valid_to_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_valid_to,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids, MONTH(last_next2_valid_to)) AS distinct_ids_last_next2_valid_to_month_sum_disti,
        FIRST_VALUE(total_rate) IGNORE NULLS OVER (ORDER BY cnt DESC, next2_valid_to_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_rate,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY distinct_ids, MONTH(last_next2_valid_to)), 2) AS cnt_vs_distinct_ids_last_next2_valid_to_month_av
    FROM stats
),
tagged AS (
    -- Derive helper columns
    SELECT
        next2_valid_to_date_ym,
        first_valid_to,
        last_next2_valid_to,
        total_rate,
        distinct_ids,
        last_first_valid_to,
        distinct_ids_last_next2_valid_to_month_sum_disti,
        first_known_total_rate,
        CONCAT(next2_valid_to_date_ym, '#', CAST(cnt AS STRING)) AS next2_valid_to_date_ym_ref
    FROM metrics
    WHERE last_next2_valid_to >= TIMESTAMP '2025-02-15 04:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        next2_valid_to_date_ym,
        first_valid_to,
        total_rate,
        distinct_ids,
        last_first_valid_to,
        first_known_total_rate,
        next2_valid_to_date_ym_ref,
        LAST_VALUE(last_next2_valid_to) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids DESC NULLS FIRST, next2_valid_to_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_next2_valid_to,
        ROUND(total_rate / NULLIF(SUM(total_rate) OVER w_distinct_ids, 0), 4) AS total_rate_share_of_distinct_ids,
        ROUND(AVG(distinct_ids) OVER w_distinct_ids, 2) AS distinct_ids_avg_distinct_ids,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids
    FROM tagged
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    next2_valid_to_date_ym,
    total_rate,
    distinct_ids,
    last_first_valid_to,
    first_known_total_rate,
    next2_valid_to_date_ym_ref,
    last_last_next2_valid_to,
    distinct_ids_avg_distinct_ids,
    rows_in_distinct_ids,
    ROUND((UNIX_TIMESTAMP(last_last_next2_valid_to) - UNIX_TIMESTAMP(first_valid_to)) / 60, 1) AS minutes_first_valid_to_to_last_last_next2_valid_
FROM windowed
ORDER BY last_last_next2_valid_to
