from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "region",
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 3)
    .select("opened_cohort", "region", "chiang_mai_count", "sqft_count")
)
