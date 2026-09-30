from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 14, 20, 40]))
    .select(
        "id",
        "email",
        F.when(F.col("level").isin(["L4", "L2"]), "Group A").otherwise("Other").alias("level_group"),
    )
    .orderBy(F.col("email").desc())
)
