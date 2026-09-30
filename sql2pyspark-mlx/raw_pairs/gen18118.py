from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "cost",
        F.dayofweek("launch_date").alias("launch_date_dayofweek"),
        (F.col("price") * F.col("stock_qty")).alias("price_times_stock_qty"),
    )
    .orderBy(F.col("id").desc())
)
