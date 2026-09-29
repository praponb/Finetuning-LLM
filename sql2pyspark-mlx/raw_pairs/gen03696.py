from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("country").desc())
    .limit(10)
)
