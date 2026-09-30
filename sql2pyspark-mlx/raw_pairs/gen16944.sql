WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        CASE WHEN revenue >= 3500 THEN 'High' WHEN revenue >= 900 THEN 'Medium' ELSE 'Low' END AS revenue_level,
        DATE_ADD(sale_date, 83) AS sale_date_plus_83d
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-07-18')
      AND units > 26
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.units,
        b.opened_date,
        b.city
    FROM filtered a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) = 1
),
deduped_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date NULLS LAST, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        units,
        RANK() OVER (PARTITION BY store_id ORDER BY units NULLS LAST) AS units_rank,
        ROW_NUMBER() OVER (ORDER BY rank_in_group DESC, id) AS row_seq,
        LEAD(units, 1, 0) OVER (PARTITION BY YEAR(opened_date) ORDER BY rank_in_group, id) AS next_units
    FROM deduped_store_sales
),
enriched_store_sales_2 AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.units,
        a.row_seq,
        a.next_units,
        s.city,
        s.store_name
    FROM enriched_store_sales a
    LEFT JOIN stores s
        ON a.store_id = s.id
    WHERE s.city = 'Berlin'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(row_seq) OVER (PARTITION BY city, store_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS FIRST) AS units_dense_rank,
        LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        SUM(next_units) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_units
    FROM enriched_store_sales_2
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        city,
        row_seq
    FROM scored
) src
PIVOT (
    MAX(row_seq) AS total
    FOR city IN ('Chiang Mai' AS chiang_mai, 'Singapore' AS singapore)
)
