from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        "salary",
        F.concat("full_name", F.lit("/"), "level").alias("full_name_level"),
    )
    .orderBy(F.col("salary").desc(), F.col("email").desc(), "id")
    .limit(3)
)
