from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val_cume_dist_2").desc_nulls_first())
w4 = Window.orderBy(F.col("val_cume_dist").desc())
w5 = Window.orderBy(F.col("val_cume_dist_2").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

filtered = (
    lines
    .filter(F.col("effective_from") >= "2025-06-01")
    .select(
        "*",
        F.round((F.col("val") * 1.15), 2).alias("adjusted_val"),
        F.date_add("effective_from", 7).alias("effective_from_plus_7d"),
    )
)

scored = (
    filtered
    .filter(F.col("val").isNotNull())
    .select(
        "effective_from",
        "adjusted_val",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.cume_dist().over(w2).alias("val_cume_dist_2"),
    )
)

metrics = (
    scored
    .select(
        "effective_from",
        "adjusted_val",
        F.percent_rank().over(w3).alias("val_cume_dist_2_percent_rank"),
        F.dense_rank().over(w4).alias("val_cume_dist_dense_rank"),
        F.rank().over(w5).alias("val_cume_dist_2_rank"),
    )
)

result = (
    metrics
    .select("effective_from", "adjusted_val", "val_cume_dist_2_rank")
    .orderBy(F.col("val_cume_dist_2_rank").desc(), "adjusted_val")
)
