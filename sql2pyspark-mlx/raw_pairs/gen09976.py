from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())

flattened = df_price_tiers.select("id", "effective_from", F.explode("tier_limits").alias("val"))

scored = flattened.withColumn("val_cume_dist", F.cume_dist().over(w))

filtered = (
    scored
    .filter(F.col("effective_from") > F.lit("2024-04-18").cast("date"))
    .withColumn("val_cume_dist_plus_val", (F.col("val_cume_dist") + F.col("val")))
)

base = (
    filtered
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-03-01")))
    .select(
        "id",
        "effective_from",
        "val",
        "val_cume_dist",
        F.when(F.col("val_cume_dist_plus_val") >= 65, "High").when(F.col("val_cume_dist_plus_val") >= 31, "Medium").otherwise("Low").alias("val_cume_dist_plus_val_band"),
        F.greatest("val", "val_cume_dist").alias("greatest_val_val_cume_dist"),
    )
)

result = (
    base
    .select(
        "id",
        "effective_from",
        "val",
        "val_cume_dist",
        "greatest_val_val_cume_dist",
    )
)
