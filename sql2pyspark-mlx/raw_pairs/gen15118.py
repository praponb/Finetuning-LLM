from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "salary",
        "dept_id",
        "hire_date",
        "email",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.first("email").over(w2).alias("first_email"),
    )
    .orderBy(F.col("email").desc())
)
