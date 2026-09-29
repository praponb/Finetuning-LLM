from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "Tokyo"]) & (F.col("city") == "Chiang Mai"))
    .select(
        "supplier_name",
        "city",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.floor("rating").alias("rating_whole"),
    )
    .orderBy("rating_whole")
)
