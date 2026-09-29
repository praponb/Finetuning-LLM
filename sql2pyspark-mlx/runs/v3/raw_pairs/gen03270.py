from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 4.0) & F.col("rating").between(2.3, 3.8) & F.col("rating").between(3.5, 3.9))
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "New York"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
    )
    .orderBy("city_group")
)
