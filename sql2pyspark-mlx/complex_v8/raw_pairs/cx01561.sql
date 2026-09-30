WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        end_date,
        status,
        DENSE_RANK() OVER (PARTITION BY holder_id, YEAR(end_date) ORDER BY end_date, id) AS end_date_dense_rank,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_id, YEAR(end_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(PERCENT_RANK() OVER w_status_by_end_date_ties, 3) AS end_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, YEAR(end_date) ORDER BY end_date, id), 3) AS end_date_percent_rank_2,
        SIZE(COLLECT_SET(holder_id) OVER w_status) AS distinct_holder_id_in_status,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        MIN(premium) OVER w_status AS status_min_premium,
        ROUND(CUME_DIST() OVER w_status_by_end_date_ties, 3) AS end_date_cume_dist,
        DENSE_RANK() OVER w_holder_id_end_date_year_by_e_3 AS end_date_dense_rank_2
    FROM base_policys
    WINDOW w_status_by_end_date_ties AS (PARTITION BY status ORDER BY end_date DESC), w_status AS (PARTITION BY status), w_holder_id AS (PARTITION BY holder_id), w_holder_id_end_date_year_by_e_3 AS (PARTITION BY holder_id, YEAR(end_date) ORDER BY end_date)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        status,
        end_date_dense_rank,
        second_premium,
        end_date_percent_rank_2,
        status_min_premium,
        end_date_cume_dist,
        end_date_dense_rank_2,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        GREATEST(end_date_percent_rank_2, end_date_dense_rank_2) AS greatest_end_date_percent_rank_2_end_date_dense_
    FROM ranked
    WHERE end_date_cume_dist < 0.7
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(end_date_percent_rank_2) OVER (PARTITION BY end_date_dense_rank) AS end_date_dense_rank_min_end_date_percent_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY end_date_dense_rank_2 ORDER BY end_date_cume_dist DESC, id), 3) AS end_date_cume_dist_cume_dist,
        ROUND(AVG(end_date_percent_rank_2) OVER (PARTITION BY end_date_dense_rank_2, end_date_dense_rank), 2) AS end_date_dense_rank_2_end_date_dense_rank_avg_en,
        ROUND(AVG(end_date_dense_rank) OVER w_end_date_dense_rank_2_end_da, 2) AS end_date_dense_rank_2_end_date_dense_rank_avg_en_2,
        LEAD(days_since_end_date, 1, 0) OVER w_end_date_dense_rank_2_by_sec AS next_days_since_end_date,
        days_since_end_date - LAG(days_since_end_date) OVER w_end_date_dense_rank_2_by_end AS days_since_end_date_change,
        LAST_VALUE(end_date_cume_dist) IGNORE NULLS OVER (PARTITION BY end_date_dense_rank ORDER BY end_date_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_cume_dist
    FROM derived
    WINDOW w_end_date_dense_rank_2_end_da AS (PARTITION BY end_date_dense_rank_2, end_date_dense_rank), w_end_date_dense_rank_2_by_sec AS (PARTITION BY end_date_dense_rank_2 ORDER BY second_premium, id), w_end_date_dense_rank_2_by_end AS (PARTITION BY end_date_dense_rank_2 ORDER BY end_date_cume_dist DESC, id)
)
SELECT
    id,
    status,
    end_date_percent_rank_2,
    status_min_premium,
    end_date_cume_dist,
    end_date_dense_rank_2,
    days_since_end_date,
    end_date_dense_rank_min_end_date_percent_rank_2,
    end_date_cume_dist_cume_dist,
    end_date_dense_rank_2_end_date_dense_rank_avg_en,
    end_date_dense_rank_2_end_date_dense_rank_avg_en_2,
    next_days_since_end_date,
    days_since_end_date_change,
    CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
    ROUND(end_date_dense_rank / NULLIF(end_date_dense_rank_2, 0), 3) AS end_date_dense_rank_per_end_date_dense_rank_2
FROM sequenced
ORDER BY end_date_percent_rank_2 DESC
