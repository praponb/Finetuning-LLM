WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        units - revenue AS revenue_per_unit,
        ADD_MONTHS(sale_date, 1) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-09-17')
      AND product_id IN (5, 8, 9)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(units) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_units,
        COUNT(units) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_units,
        SUM(units) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_units
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(running_sum_units) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_sum_units,
        LAST_VALUE(running_max_units) IGNORE NULLS OVER (PARTITION BY YEAR(sale_date), product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_max_units,
        FIRST_VALUE(revenue, TRUE) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue,
        LEAD(revenue_per_unit, 1) OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date_plus_months), store_id ORDER BY units, id) AS next_revenue_per_unit,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS row_seq
    FROM metrics
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS FIRST, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.sale_date,
        a.units,
        a.revenue,
        a.sale_date_plus_months,
        a.running_count_units,
        a.last_known_running_sum_units,
        a.first_known_revenue,
        a.row_seq,
        a.rn,
        p.sqft
    FROM deduped a
    LEFT JOIN stores p
        ON a.store_id = p.id
)
-- Multi-level aggregation
SELECT
    store_id,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(units) AS max_units
FROM joined
GROUP BY store_id;
