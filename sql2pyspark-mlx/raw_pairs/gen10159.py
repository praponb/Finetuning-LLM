from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("city").isin(["London", "Chiang Mai"]) & (F.col("store_name").like("S%") | F.col("region").isNull()))
)

summary = (
    valid_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = summary.select("opened_cohort", "city_list")
