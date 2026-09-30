from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.col("rating").between(1.9, 4.1) & (F.lower(F.trim("supplier_name")) == "global supply")) | F.col("city").isin(["London", "Berlin", "Tokyo"]))
)

rating_bucket_summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("min_rating") > 3.1)
    .orderBy("rating_bucket")
)
