from pyspark.sql import functions as F

base = df_orders.filter(F.col("store_id").isNull())

order_year_summary = (
    base
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"))
)

result = (
    order_year_summary
    .filter(F.col("completed_count") >= 4)
    .select("order_year", "completed_count")
    .orderBy("order_year")
)
