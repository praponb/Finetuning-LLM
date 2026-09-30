WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN city IN ('New York', 'Chiang Mai') THEN 'Group A' ELSE 'Other' END AS city_group
    FROM stores
    WHERE opened_date >= DATE '2023-02-17'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        region,
        opened_date,
        sqft,
        city_group,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id ASC) AS event_seq,
        DENSE_RANK() OVER w_part AS sqft_dense_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY region ORDER BY sqft ASC NULLS LAST)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY event_seq DESC NULLS LAST, id ASC) = 1
),
valid_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        region,
        opened_date,
        sqft,
        sqft_dense_rank,
        CAST(event_seq AS DOUBLE) AS event_seq_str,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM deduped
    WHERE opened_date >= TO_DATE('2023-04-04')
      AND region IS DISTINCT FROM 'Central'
)
SELECT
    id,
    region,
    opened_date,
    sqft,
    sqft_dense_rank,
    region_filled,
    CASE WHEN region RLIKE '^(Central|North)$' THEN 'GROUP_A' WHEN region RLIKE '^(South|East)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS region_zone
FROM valid_stores
