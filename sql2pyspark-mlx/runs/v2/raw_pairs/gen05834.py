from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        "store_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(F.avg("revenue").alias("avg_revenue"))
    .orderBy(F.col("store_id").desc(), "product_id", "sale_month")
    .limit(50)
)
