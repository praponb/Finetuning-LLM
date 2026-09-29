WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE product IN ('AUTO', 'LIFE', 'HOME')
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.product_prefix,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(holder_risk_score) OVER (PARTITION BY holder_region, holder_id) AS holder_region_holder_id_min_holder_risk_score,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY start_date, id), 3) AS start_date_cume_dist,
        RANK() OVER (ORDER BY holder_risk_score DESC) AS holder_risk_score_rank,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY start_date, id)) AS days_since_prev_start_date,
        ROW_NUMBER() OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY start_date, id) AS rn,
        NTILE(3) OVER (ORDER BY start_date, id) AS start_date_tercile,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date), 3) AS start_date_percent_rank,
        SUM(holder_risk_score) OVER w_end_date_year_holder_region AS end_date_year_holder_region_sum_holder_risk_scor
    FROM with_parent_holder
    WINDOW w_end_date_year_holder_region AS (PARTITION BY YEAR(end_date), holder_region)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
),
per_group AS (
    -- Group-level totals
    SELECT
        holder_id,
        SUM(rn) AS total_rn,
        ARRAY_SORT(COLLECT_SET(holder_region)) AS holder_region_list,
        SUM(start_date_tercile) AS total_start_date_tercile,
        SUM(start_date_cume_dist) AS total_start_date_cume_dist,
        ROUND(SUM(start_date_cume_dist) / NULLIF(SUM(end_date_year_holder_region_sum_holder_risk_scor), 0), 3) AS start_date_cume_dist_to_end_date_year_holder_reg,
        SUM(holder_risk_score) AS total_holder_risk_score
    FROM top_per_group
    GROUP BY holder_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(total_start_date_cume_dist, 2) OVER w_holder_id_by_total_start_dat AS prev2_total_start_date_cume_dist,
        SUM(total_start_date_cume_dist) OVER (PARTITION BY holder_id) AS holder_id_sum_total_start_date_cume_dist
    FROM per_group
    WINDOW w_holder_id_by_total_start_dat AS (PARTITION BY holder_id ORDER BY total_start_date_tercile ASC, holder_id)
)
-- Final projection
SELECT
    holder_id,
    holder_region_list,
    total_start_date_tercile,
    total_start_date_cume_dist,
    start_date_cume_dist_to_end_date_year_holder_reg,
    prev2_total_start_date_cume_dist,
    holder_id_sum_total_start_date_cume_dist,
    LEAST(total_rn, total_holder_risk_score) AS least_total_rn_total_holder_risk_score,
    COALESCE(total_start_date_tercile, 0) AS total_start_date_tercile_filled
FROM with_windows
ORDER BY total_start_date_tercile_filled DESC NULLS FIRST
