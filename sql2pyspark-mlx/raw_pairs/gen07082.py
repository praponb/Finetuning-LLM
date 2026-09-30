from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy(
        "ship_city",
        F.when(F.col("weight_kg") >= 4.2, "HIGH").otherwise("LOW").alias("weight_kg_tier"),
    )
    .agg(
        F.round(F.sum("weight_kg") / F.nullif(F.count(F.when(F.col("ship_city") == "Tokyo", 1)), F.lit(0)), 2).alias("weight_kg_per_tokyo"),
        F.max(F.when(F.col("ship_city") == "Singapore", F.col("weight_kg"))).alias("singapore_max_weight_kg"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
    )
    .filter(F.col("weight_kg_per_tokyo") >= 0)
    .orderBy(F.col("ship_city").desc())
)
