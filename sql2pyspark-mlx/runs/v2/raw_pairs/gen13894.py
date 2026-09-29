from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%ive%") & (F.col("sqft") >= 14442) & F.col("opened_date").between("2023-12-01", "2024-02-01"))
    .select(
        "id",
        "sqft",
        "city",
        "store_name",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.when(F.col("region").isin(["North", "Central"]), "Group A").otherwise("Other").alias("region_group"),
    )
    .orderBy("city", "id")
    .limit(10)
)
