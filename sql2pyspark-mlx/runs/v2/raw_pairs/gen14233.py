from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "manager_id",
        "salary",
        F.percent_rank().over(w).alias("salary_pct_rank"),
    )
)
