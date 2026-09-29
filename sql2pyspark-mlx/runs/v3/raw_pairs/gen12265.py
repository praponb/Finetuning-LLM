from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 150)
    .select(
        F.col("last_restock").alias("event_date"),
        F.lit("inventory").alias("source"),
    )
    .unionByName(
        df_products
        .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("source"))
    )
)
