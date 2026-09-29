from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("k_length", "id", "k").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("k_length", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k")
w4 = Window.partitionBy("k").orderBy("is_weight_kg_avg_3", "id", "k").rowsBetween(-5, -1)
w5 = Window.partitionBy("k").orderBy("first_known_k_length", "id", "k")
w6 = Window.partitionBy("k").orderBy(F.col("k_avg_is_weight_kg_avg_3").desc(), "id", "k")

exploded = df_carts.select("id", F.explode("attributes").alias("k", "value"))

base = (
    exploded
    .filter(F.col("k").isin(["gift", "utm_source"]))
    .withColumn("k_upper", F.upper("k"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

filtered = (
    base
    .filter(F.col("k").isNotNull() & (F.col("k").eqNullSafe(F.lit("device"))))
    .select(
        "id",
        "k",
        "value",
        "k_upper",
        "resolved_k",
        F.when(F.col("k") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
        F.length("k").alias("k_length"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("is_weight_kg").over(w).alias("is_weight_kg_avg_3"),
        F.first("k_length", ignorenulls=True).over(w2).alias("first_known_k_length"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
    )
)

cleaned = (
    enriched
    .filter(F.col("first_known_k_length") >= 72)
    .withColumn("is_weight_kg_plus_k_length", (F.col("is_weight_kg") + F.col("k_length")))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

with_windows = (
    cleaned
    .withColumn("k_avg_is_weight_kg_avg_3", F.avg("is_weight_kg_avg_3").over(w3))
    .withColumn("is_weight_kg_avg_5_prior", F.avg("is_weight_kg").over(w4))
)

result = (
    with_windows
    .filter(F.col("first_known_k_length").between(27, 52))
    .select(
        "id",
        "k",
        "value",
        "resolved_k",
        "is_weight_kg_avg_5_prior",
        F.sum("is_weight_kg_avg_3").over(w3).alias("k_sum_is_weight_kg_avg_3"),
        F.ntile(4).over(w5).alias("first_known_k_length_bucket"),
        F.ntile(10).over(w6).alias("k_avg_is_weight_kg_avg_3_bucket"),
    )
)
