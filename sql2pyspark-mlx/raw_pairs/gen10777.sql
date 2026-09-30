WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        units,
        DATEDIFF(CURRENT_DATE(), sale_date) days_since_sale_date,
        ADD_MONTHS(sale_date, 6) sale_date_plus_months
    FROM store_sales
    WHERE store_id IN (1, 6)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(days_since_sale_date) OVER w_recent next_days_since_sale_date,
        LAG(days_since_sale_date) OVER w_recent prev_days_since_sale_date,
        LEAD(units) OVER w_recent next_units,
        LAG(units) OVER w_recent prev_units,
        ROW_NUMBER() OVER w_recent seq_no
    FROM base
    WHERE days_since_sale_date != 24
    WINDOW w_recent AS (ORDER BY days_since_sale_date, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(units, 1) OVER (ORDER BY days_since_sale_date, id) next_units_2,
        ROW_NUMBER() OVER (ORDER BY next_units DESC, id) event_seq,
        LEAD(next_units, 1, 0) OVER (PARTITION BY DATE_TRUNC('month', sale_date_plus_months) ORDER BY seq_no, id) next_next_units,
        AVG(next_units) OVER (PARTITION BY DATE_TRUNC('month', sale_date_plus_months) ORDER BY seq_no, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) next_units_avg_6_prior
    FROM metrics
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        days_since_sale_date,
        prev_days_since_sale_date,
        seq_no,
        next_units_2,
        event_seq,
        next_next_units,
        next_units_avg_6_prior,
        LAG(prev_units, 1, 0) OVER (ORDER BY sale_date_plus_months, id) prev_prev_units,
        AVG(prev_days_since_sale_date) OVER (ORDER BY prev_units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) prev_days_since_sale_date_avg_6,
        FIRST_VALUE(next_days_since_sale_date) IGNORE NULLS OVER (ORDER BY prev_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_next_days_since_sale_date,
        NTILE(3) OVER (ORDER BY prev_units, id) prev_units_bucket
    FROM enriched
)
SELECT
    id,
    days_since_sale_date,
    seq_no,
    next_next_units,
    prev_days_since_sale_date_avg_6,
    prev_units_bucket
FROM enriched_store_sales
ORDER BY days_since_sale_date;
