from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val_scaled_plus_val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

valid = (
    exploded
    .filter(F.col("currency") == "THB")
    .withColumn("val_scaled", F.round((F.col("val") * 1.1), 2))
)

base = (
    valid
    .filter(F.col("currency") == "THB")
    .withColumn("val_scaled_plus_val", (F.col("val_scaled") + F.col("val")))
)

base_price_tiers = (
    base
    .filter(F.col("val_scaled").between(61, 74))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

with_windows = (
    base_price_tiers
    .filter(F.col("val_scaled") > 44)
    .select(
        "id",
        "currency",
        "val",
        "val_scaled",
        "val_scaled_plus_val",
        "plan_title",
        F.dense_rank().over(w).alias("val_scaled_plus_val_dense_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
    )
)

base_price_tiers_2 = (
    with_windows
    .filter(F.col("val_scaled_plus_val_dense_rank") == 5)
    .withColumn("greatest_val_val_scaled", F.greatest("val", "val_scaled"))
)

result = (
    base_price_tiers_2
    .select(
        "id",
        "val",
        "val_scaled",
        "val_scaled_plus_val",
        "plan_title",
        "val_scaled_plus_val_dense_rank",
        "greatest_val_val_scaled",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
