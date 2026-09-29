from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isin(["South", "Central", "East"]) & (F.col("opened_date") > "2025-06-01") & F.col("store_name").isNotNull())
)

agg = (
    base
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)

result = (
    agg
    .filter(F.col("stddev_sqft") > 37)
    .orderBy("stddev_sqft", "opened_cohort")
    .limit(3)
)
