from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Wholesale%") & F.col("rating").between(2.0, 2.9) & (F.col("country") == "SG"))
)

suppliers_stats = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("chiang_mai_rating") >= 73)
    .select("country", "chiang_mai_rating")
    .orderBy("country")
)
