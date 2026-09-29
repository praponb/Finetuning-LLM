WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        ABS(premium - 3532.7) AS premium_distance,
        ABS(premium - 3465.4) AS premium_distance_2
    FROM policies
    WHERE status NOT IN ('LAPSED', 'CANCELLED')
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_distance_2,
        p.joined_on AS holder_joined_on
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        end_date,
        status,
        premium_distance_2,
        holder_joined_on,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY end_date DESC) AS end_date_dense_rank,
        MAX(premium_distance_2) OVER (PARTITION BY holder_id, YEAR(holder_joined_on)) AS holder_id_holder_joined_on_year_max_premium_dist,
        LAST_VALUE(holder_joined_on) OVER (PARTITION BY status ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_joined_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(end_date), holder_id ORDER BY holder_joined_on DESC), 3) AS holder_joined_on_percent_rank,
        RANK() OVER (PARTITION BY YEAR(end_date), holder_id ORDER BY holder_joined_on DESC) AS holder_joined_on_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date), holder_id ORDER BY holder_joined_on DESC) AS holder_joined_on_dense_rank,
        ROUND(CUME_DIST() OVER w_status_by_premium_distance_2, 3) AS premium_distance_2_cume_dist,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        COUNT(*) OVER (PARTITION BY holder_id, YEAR(holder_joined_on)) AS rows_in_holder_id_holder_joined_on_year,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY holder_joined_on, id) AS holder_joined_on_seq,
        FIRST_VALUE(premium_distance_2) OVER (PARTITION BY status ORDER BY holder_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_distance_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(end_date), holder_id ORDER BY holder_joined_on DESC, id) AS seq_no,
        LAG(holder_joined_on) OVER (PARTITION BY status ORDER BY holder_joined_on, id) AS prev_holder_joined_on,
        LAG(holder_joined_on) OVER (PARTITION BY status ORDER BY premium_distance_2 DESC NULLS FIRST, id) AS prev_holder_joined_on_2,
        ROUND(AVG(premium_distance_2) OVER (PARTITION BY status ORDER BY holder_joined_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_distance_2_avg_3,
        MIN(premium_distance_2) OVER (PARTITION BY holder_id, YEAR(holder_joined_on)) AS holder_id_holder_joined_on_year_min_premium_dist
    FROM enriched
    WINDOW w_status_by_premium_distance_2 AS (PARTITION BY status ORDER BY premium_distance_2 DESC NULLS FIRST, id), w_holder_id AS (PARTITION BY holder_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(status, '#', CAST(end_date_dense_rank AS STRING)) AS status_ref,
        ROUND(seq_no / NULLIF(holder_joined_on_seq, 0), 3) AS seq_no_per_holder_joined_on_seq,
        LAST_DAY(prev_holder_joined_on_2) AS prev_holder_joined_on_2_month_end
    FROM with_trends
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        status,
        premium_distance_2,
        holder_joined_on,
        end_date_dense_rank,
        holder_id_holder_joined_on_year_max_premium_dist,
        last_holder_joined_on,
        holder_joined_on_percent_rank,
        holder_joined_on_rank,
        holder_joined_on_dense_rank,
        premium_distance_2_cume_dist,
        rows_in_holder_id,
        rows_in_holder_id_holder_joined_on_year,
        holder_joined_on_seq,
        prev_holder_joined_on,
        premium_distance_2_avg_3,
        holder_id_holder_joined_on_year_min_premium_dist,
        prev_holder_joined_on_2_month_end,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(last_holder_joined_on) ORDER BY holder_id_holder_joined_on_year_min_premium_dist, id), 3) AS holder_id_holder_joined_on_year_min_premium_dist_2,
        ROUND(AVG(holder_joined_on_rank) OVER (PARTITION BY YEAR(prev_holder_joined_on), end_date_dense_rank), 2) AS prev_holder_joined_on_year_end_date_dense_rank_a,
        ROUND(AVG(premium_distance_2_cume_dist) OVER (PARTITION BY MONTH(last_holder_joined_on) ORDER BY holder_id_holder_joined_on_year_min_premium_dist, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_2_cume_dist_avg_1_prior,
        RANK() OVER (PARTITION BY seq_no ORDER BY premium_distance_2_cume_dist) AS premium_distance_2_cume_dist_rank,
        RANK() OVER (PARTITION BY MONTH(last_holder_joined_on) ORDER BY holder_id_holder_joined_on_year_min_premium_dist, id) AS holder_id_holder_joined_on_year_min_premium_dist_3,
        ROUND(AVG(holder_joined_on_dense_rank) OVER (ORDER BY premium_distance_2 DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS holder_joined_on_dense_rank_avg_3
    FROM labeled
)
-- Final projection
SELECT
    id,
    holder_id,
    status,
    holder_joined_on,
    end_date_dense_rank,
    holder_id_holder_joined_on_year_max_premium_dist,
    last_holder_joined_on,
    holder_joined_on_percent_rank,
    holder_joined_on_dense_rank,
    rows_in_holder_id,
    prev_holder_joined_on,
    holder_id_holder_joined_on_year_min_premium_dist,
    prev_holder_joined_on_2_month_end,
    holder_id_holder_joined_on_year_min_premium_dist_2,
    prev_holder_joined_on_year_end_date_dense_rank_a,
    premium_distance_2_cume_dist_rank,
    holder_id_holder_joined_on_year_min_premium_dist_3,
    holder_joined_on_dense_rank_avg_3,
    DATEDIFF(prev_holder_joined_on, last_holder_joined_on) AS days_last_holder_joined_on_to_prev_holder_joined,
    FLOOR(holder_joined_on_rank / 2) * 2 AS holder_joined_on_rank_bucket
FROM windowed
ORDER BY premium_distance_2_cume_dist_rank DESC
