from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("rating") <= 3.2)

agg = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
)

result = agg.filter(F.col("bangkok_count") >= 25)
