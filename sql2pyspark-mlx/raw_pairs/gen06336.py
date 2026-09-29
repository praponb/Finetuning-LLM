from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w_running = Window.partitionBy(F.month("effective_from")).orderBy("val")
w3 = Window.orderBy(F.col("val").asc_nulls_last())

exploded = df_price_tiers.select("id", "effective_from", F.explode("tier_limits").alias("val"))

result = (
    exploded
    .select(
        "id",
        "val",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.cume_dist().over(w_running).alias("val_cume_dist_2"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
    )
)
