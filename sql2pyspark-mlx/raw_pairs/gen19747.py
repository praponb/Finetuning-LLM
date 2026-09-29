from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock").between("2024-03-01", "2024-07-01") & (F.col("quantity") <= 200)) | (F.col("quantity") >= 220))
    .withColumn("quantity_rounded", F.round("quantity", 0))
    .withColumn("weeks_since_last_restock", (F.datediff(F.current_date(), "last_restock") / 7))
    .orderBy("product_id")
)
