from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)

result = exploded.select("id", "effective_from", F.rank().over(w_hist).alias("element_rank"))
