from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("quantity") >= 192) | F.col("last_restock").between("2023-03-01", "2023-03-25"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(F.max("last_restock_plus_months").alias("latest_last_restock_plus_months"))
)

result = (
    product_id_summary
    .orderBy(F.col("product_id").desc(), "latest_last_restock_plus_months")
    .limit(3)
)
