from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull() & (F.col("salary") >= 64000))
    .select(
        "id",
        "dept_id",
        "email",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
    .orderBy("email", "id")
    .limit(50)
)
