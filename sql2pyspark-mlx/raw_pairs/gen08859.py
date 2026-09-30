from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w_hist = Window.partitionBy(F.date_trunc("YEAR", "effective_from"), "currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    lines
    .select(
        "effective_from",
        "currency",
        "val",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.min("val").over(w_hist).alias("effective_from_period_min_val"),
    )
)
