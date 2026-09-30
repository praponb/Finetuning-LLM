select id, category, explode(tags) as tag_name from products where brand in ('Umbrella', 'Acme')
