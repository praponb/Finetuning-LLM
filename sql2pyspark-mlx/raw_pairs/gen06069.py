from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull() & (F.col("salary") <= 137000))
    .select(
        "id",
        "is_remote",
        F.round((F.col("salary") * 1.15), 2).alias("adjusted_salary"),
        F.col("salary").cast("int").alias("salary_int"),
    )
    .orderBy("salary_int")
)
