from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("amount") > 620)
    .withColumn("clean_status", F.trim("status"))
)

summary = (
    valid_payments
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    summary
    .orderBy("method")
    .limit(50)
)
