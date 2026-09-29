from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("Old%"))
        .select("city", F.lit("store").alias("source"))
    )
)
