WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE end_date IS NOT NULL
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        p.region AS holder_region,
        p.holder_name,
        p.segment AS holder_segment
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        product,
        holder_region,
        MIN(start_date) AS first_start_date,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        SUM(CASE WHEN LOWER(holder_name) LIKE '%er%' THEN 1 ELSE 0 END) AS n_lower_holder_name_like_er,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(*) FILTER (WHERE UPPER(TRIM(holder_name)) LIKE 'D%') AS n_upper_trim_holder_name_like
    FROM enriched_holder
    GROUP BY DATE_TRUNC('MONTH', start_date), product, holder_region
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE n_upper_trim_holder_name_like > (SELECT AVG(n_upper_trim_holder_name_like) FROM stats)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        start_date_month,
        product,
        holder_region,
        status_list,
        n_lower_holder_name_like_er,
        n_rows,
        distinct_status,
        n_upper_trim_holder_name_like,
        COUNT(*) OVER (PARTITION BY n_lower_holder_name_like_er, holder_region) AS rows_in_n_lower_holder_name_like_er_holder_regio,
        MAX(n_rows) OVER (PARTITION BY n_lower_holder_name_like_er, holder_region) AS n_lower_holder_name_like_er_holder_region_max_n_,
        FIRST_VALUE(n_lower_holder_name_like_er) IGNORE NULLS OVER (PARTITION BY n_rows, product ORDER BY n_rows DESC NULLS FIRST, start_date_month, product, holder_region ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_lower_holder_name_like_er,
        RANK() OVER (PARTITION BY n_rows, product ORDER BY n_rows DESC NULLS FIRST) AS n_rows_rank
    FROM filtered
)
SELECT
    start_date_month,
    product,
    holder_region,
    status_list,
    n_lower_holder_name_like_er,
    n_rows,
    distinct_status,
    first_known_n_lower_holder_name_like_er,
    n_rows_rank + n_rows AS n_rows_rank_plus_n_rows
FROM sequenced
ORDER BY product
