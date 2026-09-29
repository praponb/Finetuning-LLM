from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2025-02-15") & (F.col("region") == "North"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.max("opened_date").alias("last_opened_date"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") < 11800)
)
