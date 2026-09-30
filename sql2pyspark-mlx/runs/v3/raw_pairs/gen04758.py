from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isNotNull() | F.col("level").isin(["L1", "L2"]))
)

agg = (
    base
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.min("hire_date").alias("min_hire_date"))
)

result = (
    agg
    .select("hire_cohort", "min_hire_date")
    .orderBy("min_hire_date")
)
