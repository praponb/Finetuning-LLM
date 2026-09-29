from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .filter(F.col("city") == "Tokyo")
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.sum("age").alias("total_age"))
    .withColumn("age_rank", F.rank().over(w))
    .orderBy(F.col("total_age").desc(), "signup_cohort")
)
