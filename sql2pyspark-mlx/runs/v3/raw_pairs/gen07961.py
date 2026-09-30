from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("city") == "Singapore") & F.upper("supplier_name").like("%URO%")))
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") > 4.0)
    .orderBy(F.col("avg_rating").desc())
)
