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
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM policies
    WHERE (product IS NOT NULL OR status IS NOT NULL)
),
unique_rows AS (
    -- Keep the latest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        end_date_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY end_date DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
)
SELECT
    end_date_dayofweek,
    holder_id,
    COALESCE(product, 'ALL') AS product,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS n_rows
FROM unique_rows
GROUP BY GROUPING SETS ((end_date_dayofweek, holder_id, product), (end_date_dayofweek), ())
