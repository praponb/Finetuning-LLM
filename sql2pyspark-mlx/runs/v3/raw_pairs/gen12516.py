from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-05-01")
    .groupBy("region", F.year("opened_date").alias("opened_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)
