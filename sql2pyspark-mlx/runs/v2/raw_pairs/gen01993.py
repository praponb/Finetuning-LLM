from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.length("full_name") < 11)
    .select("id", "level", "salary", F.dense_rank().over(w).alias("dense_rnk"))
)
