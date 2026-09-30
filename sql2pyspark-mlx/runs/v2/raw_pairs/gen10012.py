from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") <= 33)
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.countDistinct("age").alias("unique_countrys"),
    )
    .orderBy("city_values")
)
