WITH scoped_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE capacity >= 4912
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        DENSE_RANK() OVER (PARTITION BY MONTH(opened_on), city ORDER BY opened_on DESC, id) AS opened_on_dense_rank,
        FIRST_VALUE(opened_on) IGNORE NULLS OVER (PARTITION BY MONTH(opened_on), city ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on,
        NTH_VALUE(opened_on, 2) OVER (PARTITION BY MONTH(opened_on), city ORDER BY opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on,
        LEAD(opened_on) OVER (PARTITION BY MONTH(opened_on), city ORDER BY opened_on DESC, id) AS next_opened_on
    FROM scoped_warehouses
),
annotated AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(next_opened_on, 'MMM yyyy') AS next_opened_on_label
    FROM sequenced
    WHERE opened_on_dense_rank <= 6
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        opened_on,
        opened_on_dense_rank,
        second_opened_on,
        next_opened_on,
        next_opened_on_label,
        ROW_NUMBER() OVER w_all_by_first_known_opened_on AS rn,
        NTH_VALUE(next_opened_on, 2) OVER (ORDER BY first_known_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_opened_on,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY MONTH(next_opened_on), opened_on_dense_rank)) AS distinct_city_in_next_opened_on_month_opened_on_,
        ROUND(AVG(opened_on_dense_rank) OVER (PARTITION BY MONTH(next_opened_on), opened_on_dense_rank), 2) AS next_opened_on_month_opened_on_dense_rank_avg_op,
        NTH_VALUE(opened_on_dense_rank, 2) OVER (ORDER BY first_known_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on_dense_rank,
        RANK() OVER (ORDER BY first_known_opened_on, id) AS first_known_opened_on_rank
    FROM annotated
    WINDOW w_all_by_first_known_opened_on AS (ORDER BY first_known_opened_on, id)
),
deduped AS (
    -- Keep the latest row per city
    SELECT
        id,
        city,
        opened_on,
        opened_on_dense_rank,
        second_opened_on,
        next_opened_on,
        next_opened_on_label,
        rn,
        second_next_opened_on,
        distinct_city_in_next_opened_on_month_opened_on_,
        next_opened_on_month_opened_on_dense_rank_avg_op,
        second_opened_on_dense_rank,
        first_known_opened_on_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY second_next_opened_on DESC, id DESC) AS rn_2
        FROM ranked
    ) ranked
    WHERE rn_2 = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        city,
        rn,
        DATE_TRUNC('MONTH', second_opened_on) AS second_opened_on_month,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(city)) AS city_list,
        ROUND(AVG(first_known_opened_on_rank), 2) AS avg_first_known_opened_on_rank,
        ROUND(SUM(second_opened_on_dense_rank) / NULLIF(SUM(first_known_opened_on_rank), 0), 3) AS second_opened_on_dense_rank_to_first_known_opene,
        SUM(second_opened_on_dense_rank) AS total_second_opened_on_dense_rank
    FROM deduped
    GROUP BY city, rn, DATE_TRUNC('MONTH', second_opened_on)
)
SELECT
    city,
    rn,
    second_opened_on_month,
    second_opened_on_dense_rank_to_first_known_opene,
    total_second_opened_on_dense_rank,
    COALESCE(rn, 0) AS rn_filled
FROM per_group
WHERE n_rows >= 4
ORDER BY rn DESC
