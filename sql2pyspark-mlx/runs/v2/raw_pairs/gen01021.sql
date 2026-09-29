SELECT country FROM suppliers WHERE country NOT IN ('US', 'JP', 'DE') INTERSECT SELECT country FROM sessions
