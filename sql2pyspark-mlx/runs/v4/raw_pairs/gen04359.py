from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.orderBy("discount_pct")
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

with_windows = (
    df_promotions
    .select(
        "*",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.ntile(5).over(w3).alias("discount_pct_bucket"),
    )
)

result = (
    with_windows
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .withColumn("next_discount_pct_tier", F.when(F.col("next_discount_pct") >= 0.32, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["channel", "year_start", "next_discount_pct_tier"], ["channel", "year_start"], []],
        "channel",
        "year_start",
        "next_discount_pct_tier",
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.grouping_id().alias("gid"),
    )
)
