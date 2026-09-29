from pyspark.sql import functions as F

result = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))
