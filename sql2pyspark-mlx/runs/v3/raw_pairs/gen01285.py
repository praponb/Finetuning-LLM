from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("segment")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)
