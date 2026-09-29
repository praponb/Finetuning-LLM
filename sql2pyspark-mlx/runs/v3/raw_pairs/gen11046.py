from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)
