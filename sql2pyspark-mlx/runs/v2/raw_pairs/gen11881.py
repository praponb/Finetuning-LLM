from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "London", "Bangkok"]) & (F.col("rating") >= 2.4))
    .groupBy("city", "country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 1.9)
)
