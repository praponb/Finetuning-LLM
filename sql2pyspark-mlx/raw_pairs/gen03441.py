from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 3) & F.col("product_id").isin([7, 8]))
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        "product_id",
    )
    .agg(F.max("units").alias("max_units"))
    .orderBy("max_units", "store_id")
)
