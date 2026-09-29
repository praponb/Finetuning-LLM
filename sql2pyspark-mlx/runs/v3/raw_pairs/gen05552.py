from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"), "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)
