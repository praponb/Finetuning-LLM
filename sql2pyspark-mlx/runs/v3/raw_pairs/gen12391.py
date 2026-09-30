from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") | F.col("email").like("%@company.co"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "segment",
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
    .orderBy("signup_cohort")
)
