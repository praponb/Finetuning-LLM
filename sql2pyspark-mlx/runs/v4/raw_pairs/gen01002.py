from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("total_count") > 5)
    .select("opened_cohort", "opened_month", "total_count", "first_opened_date")
    .orderBy("opened_month")
)
