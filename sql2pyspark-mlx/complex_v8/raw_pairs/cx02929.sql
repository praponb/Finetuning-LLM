WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
    WHERE approved_amount > 2569.2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        reported_at_label,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY YEAR(reported_at), adjuster_id), 2) AS reported_at_year_adjuster_id_avg_claimed_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY YEAR(reported_at), adjuster_id), 2) AS approved_amount_vs_reported_at_year_adjuster_id_,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY reported_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id, YEAR(reported_at)) AS rows_in_adjuster_id_reported_at_year,
        SUM(claimed_amount) OVER w_adjuster_id_reported_at_year AS adjuster_id_reported_at_year_sum_claimed_amount,
        ROW_NUMBER() OVER w_adjuster_id_reported_at_mont_2 AS seq_no,
        LEAD(approved_amount, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id ORDER BY approved_amount, id) AS next_approved_amount,
        RANK() OVER w_reported_at_month_adjuster_i_3 AS approved_amount_rank,
        RANK() OVER (PARTITION BY MONTH(reported_at), adjuster_id ORDER BY approved_amount ASC) AS approved_amount_rank_2,
        MIN(claimed_amount) OVER w_adjuster_id_reported_at_year AS adjuster_id_reported_at_year_min_claimed_amount,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_adjuster_id_by_approved_amou)) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        RANK() OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY reported_at ASC, id) AS reported_at_rank
    FROM cleaned_claims
    WINDOW w_adjuster_id_reported_at_year AS (PARTITION BY adjuster_id, YEAR(reported_at)), w_adjuster_id_reported_at_mont_2 AS (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY reported_at ASC, id), w_reported_at_month_adjuster_i_3 AS (PARTITION BY DATE_TRUNC('MONTH', reported_at), adjuster_id ORDER BY approved_amount), w_adjuster_id_by_approved_amou AS (PARTITION BY adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id)
)
SELECT
    id,
    reported_at,
    reported_at_label,
    approved_amount_vs_reported_at_year_adjuster_id_,
    claimed_amount_avg_7,
    first_approved_amount,
    rows_in_adjuster_id_reported_at_year,
    adjuster_id_reported_at_year_sum_claimed_amount,
    seq_no,
    next_approved_amount,
    approved_amount_rank,
    approved_amount_rank_2,
    adjuster_id_reported_at_year_min_claimed_amount,
    minutes_since_prev_reported_at,
    reported_at_percent_rank,
    DATE_TRUNC('DAY', reported_at) AS reported_at_day,
    HOUR(reported_at) AS reported_at_hour
FROM sequenced
WHERE first_approved_amount <= 24718.5
ORDER BY reported_at_label, id
LIMIT 5
