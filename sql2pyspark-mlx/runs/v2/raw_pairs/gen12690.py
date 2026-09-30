from pyspark.sql import functions as F

src = df_stores.filter(F.col("city") == "Tokyo")

summary = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)

result = summary.orderBy(F.col("unique_regions").desc())
