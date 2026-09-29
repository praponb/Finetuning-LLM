WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        ADD_MONTHS(start_date, 3) AS start_date_plus_months
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date > '2023-07-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date NULLS FIRST, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        status,
        end_date_month_end,
        start_date_plus_months,
        rn,
        DATEDIFF(end_date_month_end, start_date) / 7 AS weeks_start_date_to_end_date_month_end
    FROM deduped
    WHERE end_date >= TO_DATE('2023-04-19')
      AND plan = 'enterprise'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        status,
        FIRST_VALUE(weeks_start_date_to_end_date_month_end) IGNORE NULLS OVER (ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weeks_start_date_to_end_date_month_end,
        FIRST_VALUE(weeks_start_date_to_end_date_month_end) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weeks_start_date_to_end_date_month_end_2,
        FIRST_VALUE(weeks_start_date_to_end_date_month_end) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weeks_start_date_to_end_date_month_end_3
    FROM cleaned
)
SELECT
    id,
    start_date,
    status,
    first_known_weeks_start_date_to_end_date_month_end_2,
    first_known_weeks_start_date_to_end_date_month_end_3
FROM scored
ORDER BY status DESC NULLS LAST;
