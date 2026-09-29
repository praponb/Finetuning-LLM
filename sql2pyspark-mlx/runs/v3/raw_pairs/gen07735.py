from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Suppl%"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("rating_bucket")
)
