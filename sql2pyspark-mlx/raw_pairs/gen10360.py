from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 6, 7]))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

sale_quarter_summary = (
    src
    .groupBy(F.quarter("sale_date").alias("sale_quarter"), "product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    sale_quarter_summary
    .select("sale_quarter", "product_id", "avg_revenue", "last_sale_date")
    .orderBy("product_id")
)
