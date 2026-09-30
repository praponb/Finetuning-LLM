from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "New York"]) & F.col("supplier_name").like("%Glo%"))
)

suppliers_stats = (
    valid_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
)

result = (
    suppliers_stats
    .select("country", "rating_bucket")
    .orderBy("rating_bucket")
)
