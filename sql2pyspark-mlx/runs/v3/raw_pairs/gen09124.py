from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull())
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
    )
    .agg(F.countDistinct("salary").alias("unique_levels"))
    .orderBy(F.col("hire_month").desc(), F.col("unique_levels").desc())
)
