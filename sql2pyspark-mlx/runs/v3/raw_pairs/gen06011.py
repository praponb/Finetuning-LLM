from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .orderBy("opened_month", "city")
    .limit(20)
)
