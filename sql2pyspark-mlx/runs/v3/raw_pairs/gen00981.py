from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com") | F.col("is_active"))
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .orderBy(F.col("stddev_credit_limit").desc())
)
