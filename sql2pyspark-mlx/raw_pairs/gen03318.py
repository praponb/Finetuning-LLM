from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "plan")
w2 = Window.orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    flattened
    .select(
        "effective_from",
        "currency",
        F.stddev_samp("val").over(w).alias("effective_from_period_stddev_samp_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)
