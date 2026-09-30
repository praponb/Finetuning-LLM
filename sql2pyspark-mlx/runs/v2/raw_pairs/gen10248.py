from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Gmb%"))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.countDistinct("rating").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") > 3)
    .orderBy(F.col("unique_citys").desc())
)
