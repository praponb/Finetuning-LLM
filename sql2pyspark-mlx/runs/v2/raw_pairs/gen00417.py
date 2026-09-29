from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        "store_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("units_count") > 5)
)
