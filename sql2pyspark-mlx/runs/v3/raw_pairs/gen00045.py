from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy("status")
    .limit(5)
)
