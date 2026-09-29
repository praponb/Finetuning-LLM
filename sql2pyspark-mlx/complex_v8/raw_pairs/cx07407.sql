WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status IN ('EXPIRED', 'CANCELLED', 'ACTIVE')
      AND premium >= 2289.0
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        holder_segment,
        holder_joined_on,
        LEAD(holder_joined_on, 3) OVER w_holder_joined_on_month_holde AS next3_holder_joined_on,
        ROUND(PERCENT_RANK() OVER w_all_by_holder_joined_on, 3) AS holder_joined_on_percent_rank,
        LEAD(holder_joined_on) OVER (ORDER BY holder_joined_on, id) AS next_holder_joined_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_segment ORDER BY holder_joined_on), 3) AS holder_joined_on_percent_rank_2,
        LAG(holder_joined_on, 2) OVER (PARTITION BY holder_segment ORDER BY holder_joined_on, id) AS prev2_holder_joined_on,
        NTH_VALUE(holder_joined_on, 2) OVER (ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on,
        RANK() OVER (PARTITION BY holder_segment ORDER BY holder_joined_on) AS holder_joined_on_rank,
        LAG(holder_joined_on) OVER (ORDER BY holder_joined_on, id) AS prev_holder_joined_on,
        FIRST_VALUE(holder_joined_on) OVER (ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_joined_on,
        NTILE(10) OVER (PARTITION BY holder_segment ORDER BY holder_joined_on, id) AS holder_joined_on_decile,
        LAST_VALUE(holder_joined_on) IGNORE NULLS OVER (PARTITION BY MONTH(holder_joined_on), holder_id ORDER BY holder_joined_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on,
        RANK() OVER (PARTITION BY holder_segment ORDER BY holder_joined_on, id) AS holder_joined_on_rank_2
    FROM joined
    WINDOW w_holder_joined_on_month_holde AS (PARTITION BY MONTH(holder_joined_on), holder_id ORDER BY holder_joined_on DESC NULLS FIRST, id), w_all_by_holder_joined_on AS (ORDER BY holder_joined_on, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(holder_joined_on_rank / 2) * 2 AS holder_joined_on_rank_bucket,
        LEAST(holder_joined_on_decile, holder_joined_on_rank) AS least_holder_joined_on_decile_holder_joined_on_r
    FROM metrics
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        holder_segment,
        holder_joined_on,
        next3_holder_joined_on,
        holder_joined_on_percent_rank,
        prev2_holder_joined_on,
        second_holder_joined_on,
        holder_joined_on_rank,
        prev_holder_joined_on,
        first_holder_joined_on,
        holder_joined_on_decile,
        holder_joined_on_rank_2,
        holder_joined_on_rank_bucket,
        least_holder_joined_on_decile_holder_joined_on_r,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY holder_joined_on_rank_2)) AS distinct_holder_id_in_holder_joined_on_rank_2,
        MIN(holder_joined_on_rank_2) OVER (PARTITION BY MONTH(next3_holder_joined_on) ORDER BY prev2_holder_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_holder_joined_on_rank_2_so_far,
        FIRST_VALUE(holder_joined_on_rank) IGNORE NULLS OVER (PARTITION BY MONTH(next3_holder_joined_on) ORDER BY prev2_holder_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_joined_on_rank,
        ROW_NUMBER() OVER w_all_by_holder_joined_on_perc AS holder_joined_on_percent_rank_2_seq,
        COUNT(*) OVER (PARTITION BY holder_joined_on_rank_2) AS rows_in_holder_joined_on_rank_2,
        ROUND(AVG(holder_joined_on_rank) OVER (PARTITION BY MONTH(next3_holder_joined_on) ORDER BY prev2_holder_joined_on NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS holder_joined_on_rank_avg_3
    FROM annotated
    WINDOW w_all_by_holder_joined_on_perc AS (ORDER BY holder_joined_on_percent_rank_2 ASC, id)
)
SELECT
    id,
    holder_id,
    holder_segment,
    holder_joined_on,
    next3_holder_joined_on,
    holder_joined_on_percent_rank,
    prev2_holder_joined_on,
    second_holder_joined_on,
    holder_joined_on_rank,
    prev_holder_joined_on,
    first_holder_joined_on,
    holder_joined_on_decile,
    holder_joined_on_rank_2,
    holder_joined_on_rank_bucket,
    least_holder_joined_on_decile_holder_joined_on_r,
    min_holder_joined_on_rank_2_so_far,
    first_known_holder_joined_on_rank,
    holder_joined_on_percent_rank_2_seq,
    holder_joined_on_rank_avg_3,
    holder_joined_on_decile + holder_joined_on_rank_2 AS holder_joined_on_decile_plus_holder_joined_on_ra
FROM with_trends
WHERE prev_holder_joined_on BETWEEN DATE '2024-02-22' AND DATE '2025-05-01'
