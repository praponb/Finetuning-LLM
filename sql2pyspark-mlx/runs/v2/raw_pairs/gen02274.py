from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity").between(138, 140))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    summary
    .select("product_id", "quantity_range", "max_quantity")
    .orderBy(F.col("max_quantity").desc(), F.col("product_id").desc())
    .limit(50)
)
