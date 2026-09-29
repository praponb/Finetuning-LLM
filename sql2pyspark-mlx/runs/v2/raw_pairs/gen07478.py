from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull() & F.col("status").isin(["OPEN", "FROZEN"]))
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("total_count") > 3)
)
