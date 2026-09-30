from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.lower(F.trim("name")) == "mia rossi") & F.col("is_active"))
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
    )
    .orderBy("city")
)
