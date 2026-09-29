from pyspark.sql import functions as F

base = df_payments.filter(F.col("method") != "paypal")

agg = (
    base
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = agg.filter(F.col("stddev_amount") > 55)
