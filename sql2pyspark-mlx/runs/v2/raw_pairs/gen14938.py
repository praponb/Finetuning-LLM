from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("id", "level", "salary", F.rank().over(w).alias("rnk"))
)
