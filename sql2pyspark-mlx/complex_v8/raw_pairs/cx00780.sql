WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean,
        ABS(premium - 2721.8) AS premium_distance
    FROM policies
    WHERE product = 'HOME'
      AND premium > 2033.8
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_holder_id_by_premium_distanc AS seq_no,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status)) AS distinct_holder_id_in_status,
        ROUND(premium_distance - AVG(premium_distance) OVER (PARTITION BY status), 2) AS premium_distance_vs_status_avg,
        ROUND(AVG(premium_distance) OVER (PARTITION BY holder_id ORDER BY premium_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_2_prior,
        premium_distance - LAG(premium_distance) OVER w_all_by_end_date AS premium_distance_change,
        RANK() OVER (ORDER BY end_date DESC) AS end_date_rank
    FROM filtered
    WINDOW w_holder_id_by_premium_distanc AS (PARTITION BY holder_id ORDER BY premium_distance DESC, id), w_all_by_end_date AS (ORDER BY end_date DESC, id)
),
filtered_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.claim_date < DATE '2024-08-20'
        )
)
SELECT
    id,
    status,
    status_clean,
    premium_distance,
    seq_no,
    distinct_holder_id_in_status,
    premium_distance_vs_status_avg,
    premium_distance_avg_2_prior,
    premium_distance_change,
    end_date_rank
FROM filtered_2
WHERE distinct_holder_id_in_status > 4
ORDER BY premium_distance_change
