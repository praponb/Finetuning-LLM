from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("amount").alias("value"))
    .intersect(
        df_store_sales
        .filter(F.col("units").between(21, 24))
        .select(F.col("revenue").alias("value"))
    )
)
