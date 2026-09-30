from pyspark.sql import functions as F

base = (
    df_stores
    .filter(~((F.col("city") == "New York") & ((F.col("region") == "East") | F.col("region").isNull())))
)

summary = (
    base
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)

result = (
    summary
    .filter(F.col("unique_regions") > 5)
    .select("opened_month", "first_opened_date", "unique_regions")
    .orderBy("unique_regions")
)
