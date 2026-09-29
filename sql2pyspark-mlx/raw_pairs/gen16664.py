from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.month("last_restock").alias("last_restock_month"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .filter(F.col("stddev_quantity") < 65)
    .orderBy("last_restock_quarter", "last_restock_month")
    .limit(20)
)
