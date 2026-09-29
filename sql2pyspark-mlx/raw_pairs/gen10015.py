from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") != "Singapore") & (F.length("store_name") > 8))
    .groupBy(
        "city",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.when(F.col("sqft") >= 14544, "high").when(F.col("sqft") >= 3618, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum("sqft").alias("total_sqft"),
    )
)
