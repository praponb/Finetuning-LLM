WITH scored AS (
    SELECT
        id,
        customer_id,
        browser,
        country,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY TO_DATE(started_at), browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
        FIRST_VALUE(browser, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) event_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY customer_id, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country_2,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY device, customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser_2
    FROM sessions
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (10, 25, 36)
        )
),
with_windows AS (
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_event_seq,
        MIN(event_seq) OVER (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_event_seq,
        MAX(event_seq) OVER (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_event_seq,
        COUNT(event_seq) OVER (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_event_seq
    FROM not_flagged
    WHERE event_seq BETWEEN 4 AND 6
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY event_seq DESC, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_seq DESC NULLS LAST, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
metrics AS (
    SELECT
        *,
        AVG(last4_avg_event_seq) OVER (PARTITION BY country, customer_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_last4_avg_event_seq,
        COUNT(last4_avg_event_seq) OVER (PARTITION BY country, customer_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_last4_avg_event_seq,
        MIN(last4_avg_event_seq) OVER (PARTITION BY country, customer_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_last4_avg_event_seq
    FROM deduped
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.last_known_country,
    s.first_known_browser,
    s.event_seq,
    s.last_known_country_2,
    s.first_known_browser_2,
    s.last4_min_event_seq,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM metrics s;
