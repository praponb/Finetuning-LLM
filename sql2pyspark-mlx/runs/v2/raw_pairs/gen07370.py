from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1140) & (F.col("amount") >= 1220) & (F.col("amount") > 950))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy("method_values", "status")
    .limit(10)
)
