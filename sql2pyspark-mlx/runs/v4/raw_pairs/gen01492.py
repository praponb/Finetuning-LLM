from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%IAM%") & ((F.col("region") != "North") | F.col("store_name").like("%Rivers%")))
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.when(F.col("region").isin(["East", "North"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
    )
    .filter(F.col("south_count") > 53)
    .orderBy("region_group")
)
