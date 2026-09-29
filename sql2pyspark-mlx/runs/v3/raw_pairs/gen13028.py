from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("source"))
    .unionByName(
        df_store_sales
        .filter(F.col("units").isNotNull())
        .select(
        F.col("sale_date").alias("last_restock"),
        F.lit("store_sale").alias("source"),
    )
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
