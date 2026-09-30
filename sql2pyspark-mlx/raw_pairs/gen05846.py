from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2024-09-01")
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") < 37000)
    .orderBy("total_sqft", "opened_month", "opened_cohort")
    .limit(10)
)
