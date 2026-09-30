from pyspark.sql import functions as F

scoped = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%p-%"))
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
    )
)

result = (
    scoped
    .select(
        "id",
        "sku_code",
        "labels",
        F.array_join(F.array_sort(F.col("labels")), ",").alias("labels_text"),
        F.size(F.col("labels")).alias("labels_count"),
    )
    .orderBy("labels_text", "id")
    .limit(10)
)
