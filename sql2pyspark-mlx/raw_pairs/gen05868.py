from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("salary").between(109000, 142000))
    .select("id", "manager_id", "salary", F.rank().over(w).alias("rnk"))
)
