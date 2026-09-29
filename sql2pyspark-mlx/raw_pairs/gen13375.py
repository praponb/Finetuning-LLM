from pyspark.sql import functions as F

result = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))
