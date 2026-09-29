WITH recent AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on
    FROM policyholders
    WHERE joined_on IS NOT NULL
      AND LOWER(holder_name) LIKE '%be%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY joined_on ASC NULLS LAST) AS joined_on_dense_rank,
        RANK() OVER w_all_by_joined_on AS joined_on_rank,
        FIRST_VALUE(joined_on) OVER (ORDER BY joined_on ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on
    FROM recent
    WINDOW w_all_by_joined_on AS (ORDER BY joined_on ASC NULLS LAST, id)
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND YEAR(p.end_date) = 2025
              AND p.product = 'HEALTH'
        )
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.id) AS avg_policy_premium,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_end_date
    FROM without_matches s
),
unique_rows AS (
    -- Keep the latest row per region
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        joined_on_dense_rank,
        joined_on_rank,
        first_joined_on,
        policy_count,
        avg_policy_premium,
        last_policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_policy_end_date DESC, id DESC) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        joined_on_dense_rank,
        first_joined_on,
        last_policy_end_date,
        ROUND(joined_on_rank / NULLIF(SUM(joined_on_rank) OVER (PARTITION BY joined_on_rank, joined_on_dense_rank), 0), 4) AS joined_on_rank_share_of_joined_on_rank_joined_on,
        DENSE_RANK() OVER (PARTITION BY region, joined_on_dense_rank ORDER BY first_joined_on ASC) AS first_joined_on_dense_rank,
        MAX(policy_count) OVER (PARTITION BY region, joined_on_dense_rank ORDER BY first_joined_on ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS policy_count_max_6_prior,
        ROUND(AVG(avg_policy_premium) OVER w_joined_on_rank_joined_on_den, 2) AS joined_on_rank_joined_on_dense_rank_avg_avg_poli
    FROM unique_rows
    WINDOW w_joined_on_rank_joined_on_den AS (PARTITION BY joined_on_rank, joined_on_dense_rank)
)
SELECT
    id,
    holder_name,
    joined_on,
    joined_on_dense_rank,
    first_joined_on,
    last_policy_end_date,
    joined_on_rank_share_of_joined_on_rank_joined_on,
    first_joined_on_dense_rank,
    policy_count_max_6_prior
FROM metrics
