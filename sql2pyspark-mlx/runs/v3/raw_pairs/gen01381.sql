select region from stores where opened_date >= '2024-06-01' intersect select region from stores where year(opened_date) >= 2025;
