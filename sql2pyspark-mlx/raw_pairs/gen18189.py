from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

result = (
    filtered
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
    .orderBy(F.col("jp_rows").desc())
)
