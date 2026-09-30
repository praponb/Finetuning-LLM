WITH stacked AS (
    -- Stack the branches
    SELECT id, birth_year AS ref_id, signup_date
    FROM listeners
    UNION
    SELECT bpm AS id, artist_id AS ref_id, release_date AS signup_date
    FROM tracks
    WHERE release_date IS NOT NULL
)
SELECT id, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(signup_date) AS latest_signup_date
FROM stacked
GROUP BY id
ORDER BY id
