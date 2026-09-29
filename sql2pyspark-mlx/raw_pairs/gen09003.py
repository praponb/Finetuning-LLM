from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary_int").desc(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-08-05").cast("date")) & (F.col("dept_id") == 12))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.col("salary").cast("int").alias("salary_int"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "email", "email_user", "salary_int")
)
