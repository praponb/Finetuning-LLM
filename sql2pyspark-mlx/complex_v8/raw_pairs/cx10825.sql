WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-02-14'
),
totals AS (
    -- Group-level totals
    SELECT
        product,
        YEAR(start_date) AS start_date_year,
        holder_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned
    GROUP BY product, YEAR(start_date), holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) AS policy_count,
        COUNT(*) AS policy_count_2,
        SUM(premium) AS total_policy_premium
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2,
        p.total_policy_premium
    FROM totals s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_3,
        (SELECT SUM(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS total_policy_premium_2
    FROM summarized_rows s
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND UPPER(TRIM(p.holder_name)) LIKE 'G%'
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        product,
        start_date_year,
        holder_id,
        cnt,
        policy_count,
        total_policy_premium,
        policy_count_3,
        ROUND(CUME_DIST() OVER w_all_by_policy_count_ties, 3) AS policy_count_cume_dist,
        SUM(policy_count_3) OVER (PARTITION BY distinct_ids) AS distinct_ids_sum_policy_count_3,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY distinct_ids)) AS distinct_holder_id_in_distinct_ids,
        NTH_VALUE(start_date_year, 2) OVER (ORDER BY policy_count, product, start_date_year, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_year,
        total_policy_premium_2 - LAG(total_policy_premium_2) OVER (ORDER BY policy_count, product, start_date_year, holder_id) AS total_policy_premium_2_change
    FROM filtered
    WINDOW w_all_by_policy_count_ties AS (ORDER BY policy_count)
)
-- Native SQL UNPIVOT (wide to long)
SELECT product, start_date_year, metric_name, amount
FROM (
    SELECT product, start_date_year, cnt, rows_in_distinct_ids
    FROM metrics
) src
UNPIVOT (
    amount FOR metric_name IN (cnt, rows_in_distinct_ids)
) unp
