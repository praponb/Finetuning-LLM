from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([30, 31, 33, 34]))
    .withColumn("shipped_date_day", F.dayofmonth("shipped_date"))
)

delivered_month_summary = (
    valid_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    delivered_month_summary
    .select("delivered_month", "status_list")
    .orderBy("delivered_month")
)
