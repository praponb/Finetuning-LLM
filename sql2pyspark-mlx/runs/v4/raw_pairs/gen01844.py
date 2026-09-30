from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date")

result = (
    df_employees
    .select("id", "hire_date", F.percent_rank().over(w).alias("hire_date_pct_rank"))
)
