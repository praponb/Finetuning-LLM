from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("created_at_quarter").desc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

lines = (
    exploded
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

filtered = (
    lines
    .filter(F.col("element").isNotNull() & (F.col("created_at") >= "2025-09-01"))
    .select("*", F.lower("element").alias("element_lower"))
)

base = (
    filtered
    .filter((~F.col("element").eqNullSafe(F.lit("VIP"))) & F.hour("created_at").between(10, 16))
    .select("*", F.quarter("created_at").alias("created_at_quarter"))
)

filtered_carts = (
    base
    .filter(~F.col("element").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("created_at_quarter_band", F.when(F.col("created_at_quarter") >= 66, "High").when(F.col("created_at_quarter") >= 28, "Medium").otherwise("Low"))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

result = (
    filtered_carts
    .filter(F.col("created_at") >= "2023-09-01")
    .select(
        "*",
        F.avg("created_at_quarter").over(w).alias("created_at_quarter_avg_30m"),
        F.max("created_at_quarter").over(w2).alias("created_at_quarter_max_30m"),
        F.dense_rank().over(w3).alias("created_at_quarter_dense_rank"),
    )
)
