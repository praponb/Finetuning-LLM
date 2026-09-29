from pyspark.sql import functions as F

base = df_stores.filter(F.col("region").isin(["Central", "West"]))

summary = (
    base
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
)

result = (
    summary
    .filter(F.col("row_count") > 4)
    .orderBy(F.col("london_count").desc())
)
