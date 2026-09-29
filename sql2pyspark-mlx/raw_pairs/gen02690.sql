WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        DATE_ADD(opened_date, 62) AS opened_date_plus_62d,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE opened_date > TO_DATE('2023-09-17')
)
SELECT
    *,
    ROW_NUMBER() OVER w_recent AS row_seq,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date_plus_62d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
    ROW_NUMBER() OVER w_recent AS row_seq_2
FROM cleaned
WINDOW w_recent AS (PARTITION BY region ORDER BY opened_date DESC, id ASC);
