from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.month("opened_date").isin([2, 3, 12]) | ~F.col("city").isin(["New York", "Tokyo", "Bangkok"])))
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        "region",
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("pct_singapore") < 77)
)
