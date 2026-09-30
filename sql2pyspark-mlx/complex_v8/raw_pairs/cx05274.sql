WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE YEAR(start_date) = 2025
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id, product ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        NTILE(3) OVER (PARTITION BY holder_id, product ORDER BY end_date, id) AS end_date_tercile,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id, product ORDER BY end_date, id)) AS days_since_prev_end_date,
        RANK() OVER (PARTITION BY holder_id, product ORDER BY end_date) AS end_date_rank
    FROM joined_holder
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        end_date,
        status,
        holder_name,
        first_known_end_date,
        end_date_tercile,
        end_date_rank,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM sequenced
)
SELECT
    end_date_rank,
    end_date_tercile,
    status,
    GROUPING(end_date_rank) AS is_total_end_date_rank,
    SUM(CASE WHEN holder_name IS NOT NULL THEN end_date_rank ELSE 0 END) AS end_date_rank_holder_name_is_not_null,
    SUM(end_date_tercile) AS total_end_date_tercile,
    ROUND(100 * COUNT(CASE WHEN status IN ('EXPIRED', 'CANCELLED') THEN 1 END) / COUNT(*), 1) AS pct_status_in_expired_cancelled
FROM tagged
GROUP BY ROLLUP(end_date_rank, end_date_tercile, status)
ORDER BY end_date_rank NULLS LAST, end_date_tercile NULLS LAST, status NULLS LAST
