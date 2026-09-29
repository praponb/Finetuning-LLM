from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") > 160)
    .select("last_restock_month", "store_id", "min_quantity")
    .orderBy("last_restock_month")
)
