from pyspark.sql import functions as F

exploded = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

result = exploded.select("value")
