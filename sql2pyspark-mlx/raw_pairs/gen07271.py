from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .groupingSets(
        [["city", "country", "brand"], ["city"], []],
        F.col("s.city").alias("city"),
        F.col("s.country").alias("country"),
        F.col("src.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.stock_qty")), 2).alias("total_stock_qty"),
    )
    .orderBy(F.col("total_stock_qty").asc_nulls_last())
)
