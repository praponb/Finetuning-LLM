from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("quantity") >= 192) | F.col("last_restock").between("2023-03-01", "2023-03-25"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

summary = (
    src
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    summary
    .select("store_id", "total_count", "last_last_restock")
    .orderBy(F.col("total_count").desc())
)
