select browser from sessions where country in ('TH', 'DE') except select browser from sessions where browser not in ('Chrome', 'Edge')
