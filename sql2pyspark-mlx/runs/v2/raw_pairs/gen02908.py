from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .groupBy("is_active")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .filter(F.col("stddev_credit_limit") < 31)
    .select("is_active", "segment_values", "stddev_credit_limit")
)
