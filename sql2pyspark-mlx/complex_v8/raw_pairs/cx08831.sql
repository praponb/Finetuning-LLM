SELECT debut_year AS entity_id, country
FROM artists
MINUS
SELECT ms_played, source
FROM plays
