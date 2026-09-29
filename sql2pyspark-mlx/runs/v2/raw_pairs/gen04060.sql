SELECT id, browser, device FROM sessions WHERE device = 'desktop' AND country IN ('DE', 'JP', 'TH') AND country NOT IN ('DE', 'JP', 'UK') ORDER BY device DESC, id LIMIT 50
