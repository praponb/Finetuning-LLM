from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([4, 14, 19, 40]) & F.col("dept_id").isin([12, 31]))
    .select(
        "id",
        "full_name",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)
