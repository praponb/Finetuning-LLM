WITH base_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
),
per_group AS (
    -- Summarise per group
    SELECT
        city,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE wh_code IS NOT NULL) AS n_wh_code_is_not_null
    FROM base_warehouses
    GROUP BY city, DATE_TRUNC('MONTH', opened_on)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        city,
        opened_on_month,
        distinct_ids,
        n_wh_code_is_not_null,
        NTH_VALUE(opened_on_month, 2) OVER (PARTITION BY MONTH(opened_on_month) ORDER BY distinct_ids DESC, city, opened_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_on_month,
        ROUND(AVG(n_wh_code_is_not_null) OVER (PARTITION BY distinct_ids ORDER BY n_wh_code_is_not_null DESC, city, opened_on_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS n_wh_code_is_not_null_avg_3_prior,
        NTILE(4) OVER w_distinct_ids_by_n_wh_code_is_2 AS n_wh_code_is_not_null_quartile,
        DENSE_RANK() OVER (PARTITION BY distinct_ids ORDER BY n_wh_code_is_not_null DESC, city, opened_on_month) AS n_wh_code_is_not_null_dense_rank
    FROM per_group
    WINDOW w_distinct_ids_by_n_wh_code_is_2 AS (PARTITION BY distinct_ids ORDER BY n_wh_code_is_not_null DESC, city, opened_on_month)
),
enriched AS (
    -- Derive helper columns
    SELECT
        city,
        opened_on_month,
        distinct_ids,
        n_wh_code_is_not_null,
        second_opened_on_month,
        n_wh_code_is_not_null_avg_3_prior,
        n_wh_code_is_not_null_dense_rank,
        LEAST(distinct_ids, n_wh_code_is_not_null) AS least_distinct_ids_n_wh_code_is_not_null
    FROM sequenced
),
labeled AS (
    -- Add calculated columns
    SELECT
        city,
        opened_on_month,
        distinct_ids,
        n_wh_code_is_not_null,
        second_opened_on_month,
        n_wh_code_is_not_null_dense_rank,
        least_distinct_ids_n_wh_code_is_not_null,
        CONCAT(city, '#', CAST(distinct_ids AS STRING)) AS city_ref
    FROM enriched
    WHERE n_wh_code_is_not_null_avg_3_prior < 8.0
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        city,
        opened_on_month,
        distinct_ids,
        n_wh_code_is_not_null,
        second_opened_on_month,
        n_wh_code_is_not_null_dense_rank,
        least_distinct_ids_n_wh_code_is_not_null,
        city_ref,
        COALESCE(distinct_ids, 0) AS distinct_ids_filled
    FROM labeled
    WHERE n_wh_code_is_not_null_dense_rank <= 8
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(n_wh_code_is_not_null, 0) AS n_wh_code_is_not_null_filled,
        CONCAT(city_ref, '#', CAST(n_wh_code_is_not_null AS STRING)) AS city_ref_ref
    FROM enriched_2
    WHERE n_wh_code_is_not_null_dense_rank BETWEEN 4 AND 8
)
SELECT
    n_wh_code_is_not_null,
    city,
    n_wh_code_is_not_null_filled,
    GROUPING(n_wh_code_is_not_null) AS is_total_n_wh_code_is_not_null,
    SUM(distinct_ids_filled) AS total_distinct_ids_filled,
    ROUND(100 * COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) / COUNT(*), 1) AS pct_city_bangkok
FROM derived
GROUP BY CUBE(n_wh_code_is_not_null, city, n_wh_code_is_not_null_filled)
ORDER BY n_wh_code_is_not_null NULLS LAST, city NULLS LAST, n_wh_code_is_not_null_filled NULLS LAST
