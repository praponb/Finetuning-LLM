WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN device IN ('mobile', 'desktop') THEN 'Group A' ELSE 'Other' END AS device_group
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-09-22 02:00:00'
      AND HOUR(started_at) BETWEEN 10 AND 13
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec BETWEEN 395 AND 409
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER w_hist AS row_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY country, customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_3
    FROM not_flagged
    WINDOW w_hist AS (PARTITION BY MONTH(started_at) ORDER BY started_at ASC, id ASC)
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.age,
        p.credit_limit
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    id,
    device,
    browser,
    started_at,
    country,
    row_seq,
    last_known_country,
    last_known_country_2,
    last_known_country_3
FROM joined
ORDER BY browser DESC, row_seq ASC;
