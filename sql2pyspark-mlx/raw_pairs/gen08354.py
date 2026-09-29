from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    cleaned
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("num_cities") < 1000)
    .orderBy(F.col("us_rows").desc_nulls_last())
)
