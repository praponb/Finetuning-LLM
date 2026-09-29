from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 28]))
    .select(
        "id",
        "salary",
        "manager_id",
        "email",
        F.length("level").alias("level_length"),
    )
)
