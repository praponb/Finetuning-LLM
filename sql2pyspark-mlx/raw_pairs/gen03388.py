from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units").between(16, 18)), "left_anti")
)

result = (
    eligible
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 16690, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft"))).alias("north_total_sqft"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
)
