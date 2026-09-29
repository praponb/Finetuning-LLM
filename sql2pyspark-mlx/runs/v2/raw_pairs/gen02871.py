from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(31, 48) & (F.col("carrier") != "DHL") & ~F.col("status").isin(["IN_TRANSIT", "DELIVERED", "RETURNED"]))
    .groupBy("status", F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("pct_delivered") >= 77)
    .orderBy(F.col("pct_delivered").desc())
)
