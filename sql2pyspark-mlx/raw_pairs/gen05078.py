from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

result = exploded.select("id", "plan", F.rank().over(w).alias("val_rank"))
