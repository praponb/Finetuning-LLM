from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = lines.select("customer_id", "coupon_codes", "price", "promo")
