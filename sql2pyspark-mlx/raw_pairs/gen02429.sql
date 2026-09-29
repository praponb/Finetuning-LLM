WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(starts_on), channel ORDER BY ends_on, id) AS event_seq
    FROM promotions
    WHERE channel IN ('mobile', 'store', 'partner')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(DATEDIFF(ends_on, starts_on) / 30, 1) AS months_starts_on_to_ends_on
    FROM metrics
    WHERE channel IS NOT NULL
      AND prev_discount_pct BETWEEN 0.29 AND 0.3
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        starts_on,
        prev_discount_pct,
        row_seq,
        months_starts_on_to_ends_on,
        FIRST_VALUE(months_starts_on_to_ends_on, TRUE) OVER (PARTITION BY promo_code ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_months_starts_on_to_ends_on,
        RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_rank,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY row_seq) AS row_seq_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY row_seq DESC, id) AS event_seq_2
    FROM cleaned
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        starts_on,
        prev_discount_pct,
        row_seq,
        row_seq_dense_rank,
        event_seq_2,
        PERCENT_RANK() OVER (ORDER BY prev_discount_pct DESC NULLS LAST) AS prev_discount_pct_percent_rank,
        RANK() OVER (ORDER BY prev_discount_pct) AS prev_discount_pct_rank
    FROM enriched
),
scored_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_discount_pct_percent_rank) OVER (ORDER BY prev_discount_pct_percent_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_prev_discount_pct_percent_rank,
        MIN(prev_discount_pct_percent_rank) OVER (ORDER BY prev_discount_pct_percent_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_prev_discount_pct_percent_rank,
        COUNT(prev_discount_pct_percent_rank) OVER (ORDER BY prev_discount_pct_percent_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_prev_discount_pct_percent_rank,
        LEAD(event_seq_2, 2) OVER (ORDER BY row_seq, id) AS next_event_seq_2
    FROM scored
)
SELECT
    id,
    prev_discount_pct,
    row_seq,
    event_seq_2,
    prev_discount_pct_rank,
    last4_sum_prev_discount_pct_percent_rank,
    last4_min_prev_discount_pct_percent_rank
FROM scored_promotions;
