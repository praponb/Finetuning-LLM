from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
    )
    .orderBy(F.col("chiang_mai_events").desc())
)
