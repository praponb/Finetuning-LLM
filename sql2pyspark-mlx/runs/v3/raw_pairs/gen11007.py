from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([12, 19, 24, 27]))
    .select(
        "id",
        "manager_id",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
    .orderBy("salary")
)
