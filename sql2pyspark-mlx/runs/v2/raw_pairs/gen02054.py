from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Gl%"))
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("unique_countrys") > 9)
    .select("city", "unique_countrys", "total_count")
)
