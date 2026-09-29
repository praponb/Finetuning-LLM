from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
)

sale_month_summary = (
    src
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        (F.max("revenue_per_unit") - F.min("revenue_per_unit")).alias("revenue_per_unit_range"),
    )
)

result = (
    sale_month_summary
    .filter(F.col("revenue_per_unit_range") < 38)
    .select("sale_month", "revenue_per_unit_range")
)
