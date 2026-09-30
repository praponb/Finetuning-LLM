from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Global%") & (F.col("rating") > 2.8) & (F.col("country") == "JP"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)
