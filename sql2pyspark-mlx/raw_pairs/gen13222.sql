WITH enriched AS (
    SELECT
        s.*,
        p.city,
        p.segment
    FROM subscriptions s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
matched_keys AS (
    SELECT id
    FROM enriched
    INTERSECT ALL
    SELECT id
    FROM subscriptions
    WHERE end_date BETWEEN '2024-04-15' AND '2025-05-01'
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    SELECT
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        city,
        segment,
        ROUND(DATEDIFF(end_date, start_date) / 30, 1) AS months_start_date_to_end_date,
        COALESCE(city, 'N/A') AS city_clean
    FROM verified
    WHERE end_date > TO_DATE('2023-07-23')
),
joined AS (
    SELECT
        src.customer_id,
        src.start_date,
        src.segment,
        src.months_start_date_to_end_date,
        p.city AS customer_city,
        p.segment AS customer_segment
    FROM filtered src
    JOIN customers p
        ON src.customer_id = p.id
),
cleaned AS (
    SELECT
        *,
        CASE WHEN months_start_date_to_end_date >= 73 THEN 'High' WHEN months_start_date_to_end_date >= 29 THEN 'Medium' ELSE 'Low' END AS months_start_date_to_end_date_band,
        LENGTH(segment) AS segment_length
    FROM joined
    WHERE start_date > DATE '2023-08-08'
)
SELECT
    segment,
    months_start_date_to_end_date,
    customer_city,
    customer_segment,
    months_start_date_to_end_date_band,
    segment_length,
    CASE WHEN customer_city RLIKE '^(Berlin|Chiang Mai)$' THEN 'OTHER_ZONE' WHEN customer_city RLIKE '^(Bangkok|New York)$' THEN 'CORE' ELSE 'GROUP_B' END AS customer_city_zone
FROM cleaned
ORDER BY segment_length DESC
