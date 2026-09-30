from pyspark.sql import functions as F

recent_payments = df_payments.filter((F.col("amount") >= 800) | F.col("amount").between(410, 640))

agg = (
    recent_payments
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count("*").alias("cnt"),
    )
)

result = (
    agg
    .filter(F.col("cnt") > 7)
    .select("method", "method_values", "cnt")
    .orderBy("cnt", "method")
)
