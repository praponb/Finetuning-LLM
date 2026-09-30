from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary")

result = (
    df_employees
    .select("id", "salary", F.percent_rank().over(w).alias("salary_pct_rank"))
)
