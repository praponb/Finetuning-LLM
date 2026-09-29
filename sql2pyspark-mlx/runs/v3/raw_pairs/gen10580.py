from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 7, 12]) & ~F.col("store_name").like("%River%"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
    )
)
