WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED', 'LAPSED')
      AND holder_id >= 3
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        ROUND(AVG(premium), 2) AS avg_policy_premium,
        MIN(end_date) AS first_policy_end_date,
        COUNT(*) FILTER (WHERE product IN ('HEALTH', 'TRAVEL')) AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.avg_policy_premium,
        p.first_policy_end_date,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM prepared s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(end_date) AS end_date_year,
        status,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(AVG(policy_product_count), 2) AS avg_policy_product_count,
        MAX(premium) AS max_premium
    FROM summarized_rows
    WHERE product_ref IS NOT NULL
    GROUP BY YEAR(end_date), status
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(max_premium) OVER (PARTITION BY distinct_holder_ids) AS distinct_holder_ids_min_max_premium,
        SUM(max_premium) OVER (ORDER BY avg_policy_product_count DESC NULLS FIRST, end_date_year, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_premium,
        MIN(end_date_year) OVER (ORDER BY max_premium, end_date_year, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_end_date_year_so_far
    FROM summary
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        end_date_year,
        status,
        distinct_holder_ids,
        avg_policy_product_count,
        max_premium,
        distinct_holder_ids_min_max_premium,
        running_max_premium,
        min_end_date_year_so_far,
        NTH_VALUE(max_premium, 2) OVER (ORDER BY running_max_premium DESC, end_date_year, status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_premium,
        SUM(running_max_premium) OVER (ORDER BY distinct_holder_ids DESC NULLS FIRST, end_date_year, status ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_max_premium_sum_3_prior,
        ROUND(max_premium / NULLIF(SUM(max_premium) OVER (PARTITION BY status, end_date_year), 0), 4) AS max_premium_share_of_status_end_date_year,
        distinct_holder_ids - LAG(distinct_holder_ids) OVER (ORDER BY distinct_holder_ids DESC NULLS FIRST, end_date_year, status) AS distinct_holder_ids_change,
        COUNT(*) OVER w_status_end_date_year AS rows_in_status_end_date_year
    FROM with_windows
    WINDOW w_status_end_date_year AS (PARTITION BY status, end_date_year)
)
-- Final projection
SELECT
    end_date_year,
    status,
    distinct_holder_ids,
    avg_policy_product_count,
    max_premium,
    second_max_premium,
    running_max_premium_sum_3_prior,
    max_premium_share_of_status_end_date_year,
    distinct_holder_ids_change,
    rows_in_status_end_date_year,
    LEAST(max_premium, min_end_date_year_so_far) AS least_max_premium_min_end_date_year_so_far,
    UPPER(TRIM(status)) AS status_clean
FROM with_trends
