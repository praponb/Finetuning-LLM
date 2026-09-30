WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date < DATE '2026-10-23'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM recent_policys
    WHERE holder_id <= 9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status, holder_id) AS rows_in_status_holder_id,
        NTILE(10) OVER w_status_product_by_start_date AS start_date_decile,
        MIN(premium) OVER (PARTITION BY product, holder_id ORDER BY days_end_date_to_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY product, holder_id ORDER BY days_end_date_to_start_date), 3) AS days_end_date_to_start_date_cume_dist,
        ROUND(days_end_date_to_start_date - AVG(days_end_date_to_start_date) OVER (PARTITION BY status, holder_id), 2) AS days_end_date_to_start_date_vs_status_holder_id_,
        ROW_NUMBER() OVER (PARTITION BY status, product ORDER BY start_date, id) AS start_date_seq
    FROM labeled
    WINDOW w_status_product_by_start_date AS (PARTITION BY status, product ORDER BY start_date, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        premium,
        status,
        product_prefix,
        product_slug,
        days_end_date_to_start_date,
        rows_in_status_holder_id,
        start_date_decile,
        min_premium_so_far,
        days_end_date_to_start_date_cume_dist,
        days_end_date_to_start_date_vs_status_holder_id_,
        start_date_seq,
        ROUND(premium * 0.9, 2) AS premium_adj,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
    FROM ranked
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status <> 'REJECTED'
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        start_date_decile,
        CASE WHEN premium >= 3817.5 THEN 'A' WHEN premium >= 1132.1 THEN 'B' ELSE 'C' END AS premium_tier,
        product,
        ROUND(AVG(rows_in_status_holder_id), 2) AS avg_rows_in_status_holder_id,
        COUNT(*) AS row_count
    FROM without_matches
    WHERE premium > 1042.8
    GROUP BY start_date_decile, CASE WHEN premium >= 3817.5 THEN 'A' WHEN premium >= 1132.1 THEN 'B' ELSE 'C' END, product
    HAVING SUM(premium) > 2096.6
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(start_date_decile, 0) AS start_date_decile_filled,
        CASE WHEN premium_tier IN ('B', 'C') THEN 'group_a' WHEN premium_tier = 'A' THEN 'group_b' ELSE 'other' END AS premium_tier_group
    FROM rollup_base
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(premium_tier_group, 'ALL') AS premium_tier_group,
    COALESCE(product, 'ALL') AS product,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    COUNT(DISTINCT premium_tier_group) AS distinct_premium_tier_groups
FROM tagged
GROUP BY GROUPING SETS ((premium_tier_group, product), (premium_tier_group), (product))
ORDER BY premium_tier_group NULLS LAST, product NULLS LAST
