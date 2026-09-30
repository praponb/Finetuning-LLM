from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating").between(2.5, 3.5) & (F.col("rating") < 3.0)) | F.lower("supplier_name").like("%iam%"))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .filter(F.col("unique_countrys") < 2)
    .orderBy(F.col("unique_countrys").desc())
)
