from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([4, 9, 12]))
    .select("hire_date", F.lit("employee").alias("kind"))
    .union(
        df_stores
        .filter(F.col("region") == "Central")
        .select("opened_date", F.lit("store").alias("kind"))
    )
    .distinct()
    .orderBy("hire_date")
)
