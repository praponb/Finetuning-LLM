from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.count("*").alias("cnt"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)
