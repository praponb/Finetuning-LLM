from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-01-05")
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") < 25)
    .orderBy("stddev_sqft", "opened_cohort")
)
