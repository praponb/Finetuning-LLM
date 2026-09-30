from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() | (F.col("city") == "Bangkok"))
)

suppliers_stats = (
    src
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("us_rating") < 73)
    .select("country", "us_rating", "sg_count")
    .orderBy("sg_count")
)
