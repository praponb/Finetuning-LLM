from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.countDistinct("age").alias("unique_countrys"))
    .filter(F.col("unique_countrys") < 7)
    .select("signup_quarter", "unique_countrys")
)
