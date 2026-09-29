from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") >= 8)
    .select("country", "segment_values", "age_count")
    .orderBy("age_count")
)
