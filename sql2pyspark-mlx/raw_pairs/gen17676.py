from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w_part = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    lines
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "plan",
        "val",
        F.rank().over(w).alias("val_rank"),
        F.rank().over(w2).alias("val_rank_2"),
        F.percent_rank().over(w_part).alias("val_percent_rank"),
    )
)
