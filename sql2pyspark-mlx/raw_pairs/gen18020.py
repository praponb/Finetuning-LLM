from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2024-08-09")
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("region").isin(["East", "South"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") < 76)
    .orderBy("opened_month", "region_group")
    .limit(20)
)
