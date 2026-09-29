from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .filter(F.col("sg_count") >= 5)
)
