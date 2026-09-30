from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 4, 10]) & (F.col("delivered_date") > "2024-12-01") & ~F.col("status").isin(["RETURNED", "IN_TRANSIT", "LOST"]))
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("total_cost").desc())
)
