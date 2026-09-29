from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
