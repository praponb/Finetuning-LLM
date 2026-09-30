from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id") == 3)
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
)

summary = (
    filtered_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.avg("revenue_per_unit"), 2).alias("avg_revenue_per_unit"),
    )
)

result = (
    summary
    .select("sale_quarter", "first_sale_date", "avg_revenue_per_unit")
    .orderBy("first_sale_date")
)
