from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "region",
    )
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .filter(F.col("west_sqft") >= 26)
    .select("opened_cohort", "region", "west_sqft")
)
