from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 122)
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("max_quantity") >= 90)
)
