from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.countDistinct("credit_limit").alias("unique_segments"),
    )
    .filter(F.col("unique_segments") > 1)
)
