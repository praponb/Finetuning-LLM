SELECT country FROM customers UNION SELECT country FROM sessions WHERE device != 'desktop' ORDER BY country DESC
