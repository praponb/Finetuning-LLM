from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("quantity") > 72)
        .select(F.col("last_restock").alias("event_date"))
    )
    .orderBy("event_date")
)
