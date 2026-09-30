from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L3", "L2"]))
    .select(
        "id",
        "level",
        "hire_date",
        "manager_id",
        "salary",
        F.percent_rank().over(w).alias("hire_date_pct_rank"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
)
