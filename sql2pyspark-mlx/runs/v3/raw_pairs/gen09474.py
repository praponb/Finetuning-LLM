from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("rating") >= 2.4)

country_summary = (
    filtered_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = country_summary.filter(F.col("avg_rating") < 2.8)
