from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity").between(101, 239))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.85), 2))
)

summary = (
    base
    .groupBy("store_id")
    .agg(
        (F.max("quantity_scaled") - F.min("quantity_scaled")).alias("quantity_scaled_range"),
        F.min("quantity_scaled").alias("min_quantity_scaled"),
    )
)

result = (
    summary
    .orderBy(
        F.col("min_quantity_scaled").desc(),
        F.col("quantity_scaled_range").desc(),
        "store_id",
    )
    .limit(5)
)
