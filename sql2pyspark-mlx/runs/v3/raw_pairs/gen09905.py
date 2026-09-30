from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 82)
    .select("last_restock", F.lit("inventory").alias("source"))
    .unionByName(
        df_products
        .select(
        F.col("launch_date").alias("last_restock"),
        F.lit("product").alias("source"),
    )
    )
    .distinct()
    .orderBy("source")
)
