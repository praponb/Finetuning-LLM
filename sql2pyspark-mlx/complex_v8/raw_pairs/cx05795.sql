WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        p.holder_name,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        start_date,
        holder_region,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC), 3) AS holder_joined_on_percent_rank,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER w_product_holder_id_by_holder_3) AS days_since_prev_holder_joined_on,
        DENSE_RANK() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY start_date, id) AS start_date_dense_rank,
        RANK() OVER w_product_holder_id_by_holder AS holder_joined_on_rank,
        NTILE(3) OVER (ORDER BY holder_joined_on DESC, id) AS holder_joined_on_tercile,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC, id)) AS days_since_prev_start_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        FIRST_VALUE(holder_joined_on) OVER (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_joined_on,
        LAG(start_date, 2) OVER (ORDER BY start_date DESC, id) AS prev2_start_date,
        ROUND(PERCENT_RANK() OVER w_all_by_holder_joined_on_ties, 3) AS holder_joined_on_percent_rank_2
    FROM with_parent_holder
    WINDOW w_product_holder_id_by_holder_3 AS (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC, id), w_product_holder_id_by_holder AS (PARTITION BY product, holder_id ORDER BY holder_joined_on DESC), w_all_by_holder_joined_on_ties AS (ORDER BY holder_joined_on DESC)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        start_date,
        holder_region,
        holder_joined_on_percent_rank,
        first_start_date,
        start_date_dense_rank,
        holder_joined_on_rank,
        holder_joined_on_tercile,
        first_known_start_date,
        second_start_date,
        first_holder_joined_on,
        prev2_start_date,
        holder_joined_on_percent_rank_2,
        SIZE(COLLECT_SET(holder_id) OVER w_holder_region) AS distinct_holder_id_in_holder_region,
        NTH_VALUE(start_date_dense_rank, 2) OVER (ORDER BY second_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_dense_rank,
        ROUND(holder_joined_on_rank / NULLIF(SUM(holder_joined_on_rank) OVER (PARTITION BY holder_region), 0), 4) AS holder_joined_on_rank_share_of_holder_region,
        ROUND(days_since_prev_start_date - AVG(days_since_prev_start_date) OVER (PARTITION BY holder_region), 2) AS days_since_prev_start_date_vs_holder_region_avg
    FROM ranked
    WINDOW w_holder_region AS (PARTITION BY holder_region)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    holder_region,
    holder_joined_on_percent_rank,
    first_start_date,
    start_date_dense_rank,
    holder_joined_on_rank,
    holder_joined_on_tercile,
    first_known_start_date,
    second_start_date,
    first_holder_joined_on,
    prev2_start_date,
    holder_joined_on_percent_rank_2,
    distinct_holder_id_in_holder_region,
    second_start_date_dense_rank,
    holder_joined_on_rank_share_of_holder_region,
    days_since_prev_start_date_vs_holder_region_avg,
    DAYOFWEEK(first_start_date) AS first_start_date_dayofweek,
    DAYOFWEEK(start_date) AS start_date_dayofweek
FROM ranked_2
WHERE days_since_prev_start_date_vs_holder_region_avg BETWEEN -140.0 AND 80.0
ORDER BY start_date_dayofweek, distinct_holder_id_in_holder_region
