WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM sites
    WHERE tariff IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(commissioned_on) OVER (PARTITION BY MONTH(commissioned_on), zone ORDER BY commissioned_on, id) AS prev_commissioned_on,
        FIRST_VALUE(commissioned_on) OVER (ORDER BY commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on
    FROM prepared
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        commissioned_on,
        tariff_ref,
        prev_commissioned_on,
        first_commissioned_on,
        RANK() OVER w_first_commissioned_on_year_b AS prev_commissioned_on_rank,
        RANK() OVER (PARTITION BY YEAR(first_commissioned_on) ORDER BY prev_commissioned_on ASC) AS prev_commissioned_on_rank_2,
        DENSE_RANK() OVER (PARTITION BY YEAR(prev_commissioned_on), zone ORDER BY commissioned_on, id) AS commissioned_on_dense_rank
    FROM with_trends
    WINDOW w_first_commissioned_on_year_b AS (PARTITION BY YEAR(first_commissioned_on) ORDER BY prev_commissioned_on ASC, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        commissioned_on,
        tariff_ref,
        prev_commissioned_on,
        first_commissioned_on,
        prev_commissioned_on_rank,
        prev_commissioned_on_rank_2,
        commissioned_on_dense_rank,
        LEAD(first_commissioned_on, 2) OVER (PARTITION BY commissioned_on_dense_rank, prev_commissioned_on_rank ORDER BY prev_commissioned_on, id) AS next2_first_commissioned_on,
        ROUND(prev_commissioned_on_rank_2 / NULLIF(SUM(prev_commissioned_on_rank_2) OVER (PARTITION BY MONTH(commissioned_on)), 0), 4) AS prev_commissioned_on_rank_2_share_of_commissione,
        ROW_NUMBER() OVER w_commissioned_on_dense_rank_p AS seq_no
    FROM ranked
    WINDOW w_commissioned_on_dense_rank_p AS (PARTITION BY commissioned_on_dense_rank, prev_commissioned_on_rank ORDER BY prev_commissioned_on, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, prev_commissioned_on_rank, seq_no, prev_commissioned_on_rank_2, commissioned_on_dense_rank
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR measure IN (prev_commissioned_on_rank, seq_no, prev_commissioned_on_rank_2, commissioned_on_dense_rank)
) unp
