from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("revenue") < 2800)
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

store_id_summary = (
    src
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    store_id_summary
    .orderBy("sale_month", "store_id")
    .limit(10)
)
