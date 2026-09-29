from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 4)
)
