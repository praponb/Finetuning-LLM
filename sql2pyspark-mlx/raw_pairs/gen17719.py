from pyspark.sql import functions as F

result = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))
