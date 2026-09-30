WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(sale_date, 43) sale_date_minus_43d
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-10-03')
      AND revenue BETWEEN 1500 AND 2400
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.sale_date,
        src.units,
        src.revenue,
        src.sale_date_minus_43d,
        p.sqft
    FROM base src
    LEFT JOIN stores p
        ON src.store_id = p.id
    WHERE p.city = 'Bangkok'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date_minus_43d DESC NULLS FIRST, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE store_id = 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date_minus_43d,
        rn,
        DENSE_RANK() OVER (ORDER BY revenue NULLS FIRST) revenue_dense_rank,
        CORR(units, revenue) OVER (PARTITION BY YEAR(sale_date), store_id) corr_units_revenue
    FROM verified
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sale_date_minus_43d,
        CUME_DIST() OVER w_ordered corr_units_revenue_cume_dist,
        PERCENT_RANK() OVER (ORDER BY corr_units_revenue) corr_units_revenue_percent_rank,
        PERCENT_RANK() OVER w_hist corr_units_revenue_percent_rank_2
    FROM scored
    WINDOW w_ordered AS (ORDER BY corr_units_revenue DESC NULLS FIRST), w_hist AS (ORDER BY corr_units_revenue NULLS LAST)
),
report AS (
    SELECT id, corr_units_revenue_percent_rank
    FROM enriched
)
SELECT id, corr_units_revenue_percent_rank
FROM report
ORDER BY corr_units_revenue_percent_rank, id
