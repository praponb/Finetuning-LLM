WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.status,
        c.age,
        c.name,
        c.segment
    FROM latest s
    LEFT JOIN customers c
        ON s.customer_id = c.id
    WHERE c.city NOT IN ('Bangkok', 'Tokyo')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        name,
        ROW_NUMBER() OVER (PARTITION BY status, segment ORDER BY age DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY customer_id ORDER BY age DESC NULLS LAST) AS age_rank
    FROM joined
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name AS customer_name,
        p.signup_date,
        p.email
    FROM with_windows s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        seq_no,
        age_rank,
        email,
        DENSE_RANK() OVER w_grp AS age_rank_dense_rank,
        NTILE(5) OVER (PARTITION BY status, customer_id ORDER BY seq_no, id) AS seq_no_bucket,
        SUM(seq_no) OVER (PARTITION BY status ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no
    FROM joined_accounts
    WINDOW w_grp AS (PARTITION BY customer_id, status ORDER BY age_rank NULLS LAST)
)
-- Running and moving aggregates
SELECT
    id,
    name,
    email,
    age_rank_dense_rank,
    seq_no_bucket,
    running_seq_no,
    LEAD(age_rank_dense_rank) OVER (ORDER BY age_rank_dense_rank, id) AS next_age_rank_dense_rank,
    LAG(age_rank_dense_rank) OVER (ORDER BY age_rank_dense_rank, id) AS prev_age_rank_dense_rank,
    LAG(age_rank) OVER (ORDER BY age_rank_dense_rank, id) AS prev_age_rank,
    LEAD(age_rank) OVER (ORDER BY age_rank_dense_rank, id) AS next_age_rank,
    ROW_NUMBER() OVER (ORDER BY age_rank_dense_rank, id) AS event_seq
FROM scored;
