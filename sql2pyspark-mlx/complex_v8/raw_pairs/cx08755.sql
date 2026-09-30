WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        end_date,
        status_len,
        ROUND(status_len / NULLIF(SUM(status_len) OVER w_status_len_product, 0), 4) AS status_len_share_of_status_len_product,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        RANK() OVER (ORDER BY start_date DESC, id) AS start_date_rank,
        MIN(status_len) OVER (PARTITION BY status_len, product) AS status_len_product_min_status_len
    FROM recent_policys
    WINDOW w_status_len_product AS (PARTITION BY status_len, product)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, status_len, start_date_rank
    FROM with_windows
) src
UNPIVOT (
    value FOR metric_name IN (status_len AS m_status_len, start_date_rank AS start_date_rank_v)
) unp
