WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        UPPER(TRIM(store_name)) store_name_normalized,
        ADD_MONTHS(opened_date, 6) opened_date_plus_months
    FROM stores
    WHERE region IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date_plus_months, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        store_name_normalized,
        opened_date_plus_months,
        rank_in_group,
        ROW_NUMBER() OVER w_hist seq_no,
        ROW_NUMBER() OVER w_hist event_seq,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_plus_months, id) event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY rank_in_group DESC, id) seq_no_2
    FROM ranked
    WINDOW w_hist AS (ORDER BY rank_in_group DESC, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY region ORDER BY seq_no, id) seq_no_bucket,
        LAST_VALUE(seq_no_2, TRUE) OVER (PARTITION BY region, city ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_seq_no_2,
        LAG(event_seq_2, 1) OVER (PARTITION BY city ORDER BY seq_no_2, id) prev_event_seq_2,
        LAST_VALUE(seq_no, TRUE) OVER w_recent last_known_seq_no,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY event_seq DESC, id) seq_no_3
    FROM metrics
    WINDOW w_recent AS (PARTITION BY city, region ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date_plus_months
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date_plus_months) AS quarter_start,
    SUM(CASE WHEN city = 'Tokyo' THEN seq_no ELSE 0 END) AS tokyo_seq_no,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN region = 'West' THEN last_known_seq_no ELSE 0 END) AS west_last_known_seq_no
FROM eligible
GROUP BY 1
HAVING tokyo_seq_no > 0
ORDER BY quarter_start NULLS FIRST;
