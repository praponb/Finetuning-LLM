from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 6)
    .select("level", "first_hire_date", "cnt")
)
