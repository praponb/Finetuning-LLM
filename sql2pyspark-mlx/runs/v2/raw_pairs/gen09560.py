from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.26) & F.col("customer_id").isin([17, 21, 30]))
    .groupBy("status")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("total_discount") >= 0.7)
)
