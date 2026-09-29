from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .filter(F.col("min_credit_limit") >= 16000)
    .select("country", "min_credit_limit", "country_values")
)
