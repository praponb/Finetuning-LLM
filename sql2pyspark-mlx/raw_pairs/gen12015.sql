WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        FLOOR(sqft) AS sqft_whole
    FROM stores
    WHERE opened_date > TO_DATE('2023-09-20')
      AND region IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
),
latest_stores AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_whole DESC NULLS FIRST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        opened_date_label,
        sqft_whole,
        row_num,
        COUNT(sqft_whole) OVER (PARTITION BY region ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft_whole,
        MAX(sqft_whole) OVER (PARTITION BY region ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft_whole,
        MIN(sqft_whole) OVER (PARTITION BY region ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft_whole,
        AVG(sqft_whole) OVER (PARTITION BY region ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_whole_avg_2
    FROM latest_stores
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock >= s.opened_date
        )
),
report AS (
    SELECT
        id,
        region,
        sqft,
        opened_date_label,
        sqft_whole,
        row_num,
        running_count_sqft_whole,
        running_max_sqft_whole,
        running_min_sqft_whole
    FROM eligible
)
SELECT
    id,
    region,
    sqft,
    row_num,
    running_min_sqft_whole
FROM report
ORDER BY running_min_sqft_whole;
