WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE status IS NOT NULL
      AND premium BETWEEN 2013.8 AND 3544.1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        status_prefix,
        end_date_label,
        RANK() OVER (PARTITION BY holder_id, product ORDER BY end_date DESC) AS end_date_rank,
        RANK() OVER (PARTITION BY MONTH(end_date), status ORDER BY end_date DESC, id) AS end_date_rank_2
    FROM filtered
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(premium / NULLIF(end_date_rank_2, 0), 3) AS premium_per_end_date_rank_2
    FROM metrics
    WHERE status_prefix IS NOT NULL
)
-- Final projection
SELECT
    id,
    premium,
    status_prefix,
    end_date_label,
    end_date_rank,
    end_date_rank_2,
    premium_per_end_date_rank_2,
    LOWER(TRIM(status_prefix)) AS status_prefix_clean,
    DATE_TRUNC('YEAR', start_date) AS start_date_year
FROM tagged
