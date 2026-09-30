from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date")

result = (
    df_employees
    .select("id", "level", "hire_date", F.rank().over(w).alias("rnk"))
    .orderBy("hire_date", F.col("id").desc())
)
