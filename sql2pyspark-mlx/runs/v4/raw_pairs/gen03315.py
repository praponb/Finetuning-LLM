from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= F.col("s.order_date")), "left_semi")
)

result = (
    kept
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"), "status")
    .agg(
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("mobile_events").asc_nulls_first())
)
