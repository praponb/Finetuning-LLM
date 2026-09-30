SELECT browser, device FROM sessions WHERE device IN ('desktop', 'mobile', 'tablet') INTERSECT SELECT browser, device FROM sessions WHERE device = 'desktop' ORDER BY browser;
