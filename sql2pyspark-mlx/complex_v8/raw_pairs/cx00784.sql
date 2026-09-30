WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM policies
    WHERE start_date IS NOT NULL
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.status_or_unknown,
        s.days_since_end_date,
        s.end_date_quarter,
        p.segment AS holder_segment,
        p.holder_name,
        p.region AS holder_region
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.holder_name IS NOT NULL
),
stats AS (
    -- Group-level totals
    SELECT
        product,
        status_or_unknown,
        holder_region,
        COUNT(*) AS cnt,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
        COUNT(DISTINCT status_or_unknown) AS distinct_status_or_unknowns,
        SUM(days_since_end_date) AS total_days_since_end_date,
        SUM(days_since_end_date) FILTER (WHERE holder_region IS NOT NULL) AS days_since_end_date_holder_region_is_not_null,
        ROUND(STDDEV(days_since_end_date), 2) AS stddev_days_since_end_date
    FROM combined_details_holder
    GROUP BY product, status_or_unknown, holder_region
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE cnt > (SELECT MIN(cnt) FROM stats)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown,
        ABS(cnt - 6) AS cnt_distance
    FROM without_matches
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        product,
        status_or_unknown,
        holder_region,
        cnt,
        distinct_holder_segments,
        distinct_status_or_unknowns,
        total_days_since_end_date,
        days_since_end_date_holder_region_is_not_null,
        stddev_days_since_end_date,
        holder_region_or_unknown,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY cnt)) AS distinct_product_in_cnt,
        DENSE_RANK() OVER (ORDER BY days_since_end_date_holder_region_is_not_null ASC NULLS LAST) AS days_since_end_date_holder_region_is_not_null_de,
        ROW_NUMBER() OVER (PARTITION BY status_or_unknown, distinct_holder_segments ORDER BY days_since_end_date_holder_region_is_not_null ASC, product, status_or_unknown, holder_region) AS seq_no,
        FIRST_VALUE(total_days_since_end_date) OVER (PARTITION BY status_or_unknown, distinct_holder_segments ORDER BY days_since_end_date_holder_region_is_not_null ASC, product, status_or_unknown, holder_region ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_days_since_end_date
    FROM derived
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(distinct_product_in_cnt, 2, 0) OVER w_all_by_days_since_end_date_h_2 AS next2_distinct_product_in_cnt,
        NTILE(4) OVER w_all_by_days_since_end_date_h_2 AS days_since_end_date_holder_region_is_not_null_qu,
        NTH_VALUE(seq_no, 2) OVER (PARTITION BY distinct_status_or_unknowns ORDER BY days_since_end_date_holder_region_is_not_null_de, product, status_or_unknown, holder_region ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_seq_no,
        NTH_VALUE(total_days_since_end_date, 2) OVER (PARTITION BY distinct_status_or_unknowns ORDER BY days_since_end_date_holder_region_is_not_null_de, product, status_or_unknown, holder_region ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_days_since_end_date,
        RANK() OVER w_distinct_status_or_unknowns_2 AS days_since_end_date_holder_region_is_not_null_de_2
    FROM with_windows
    WINDOW w_all_by_days_since_end_date_h_2 AS (ORDER BY days_since_end_date_holder_region_is_not_null, product, status_or_unknown, holder_region), w_distinct_status_or_unknowns_2 AS (PARTITION BY distinct_status_or_unknowns ORDER BY days_since_end_date_holder_region_is_not_null_de, product, status_or_unknown, holder_region)
)
SELECT
    product,
    distinct_holder_segments,
    total_days_since_end_date,
    days_since_end_date_holder_region_is_not_null,
    stddev_days_since_end_date,
    holder_region_or_unknown,
    days_since_end_date_holder_region_is_not_null_de,
    seq_no,
    first_total_days_since_end_date,
    days_since_end_date_holder_region_is_not_null_qu,
    second_seq_no,
    days_since_end_date_holder_region_is_not_null_de_2
FROM ranked
ORDER BY days_since_end_date_holder_region_is_not_null_de_2
