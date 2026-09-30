from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .groupBy("segment")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sum(F.when(F.col("city") == "London", F.col("credit_limit")).otherwise(0)).alias("london_credit_limit"),
    )
    .filter(F.col("london_credit_limit") > 58)
    .orderBy("segment")
    .limit(5)
)
