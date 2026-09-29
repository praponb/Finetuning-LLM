from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "New York"]) & F.col("is_active"))
)

summary = (
    base
    .groupBy("city")
    .agg(
        F.countDistinct("age").alias("unique_countrys"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    summary
    .select("city", "unique_countrys", "city_list")
    .orderBy("unique_countrys")
)
