from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([4, 7, 8]))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .filter(F.col("unique_countrys") > 5)
    .select("signup_month", "unique_countrys", "last_signup_date")
    .orderBy(F.col("signup_month").desc())
)
