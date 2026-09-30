from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 103000) & ((F.col("salary") <= 83000) | F.col("email").like("%@company.co")))
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.col("level").desc())
)
