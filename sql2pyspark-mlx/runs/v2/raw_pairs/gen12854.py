from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary")

result = (
    df_employees
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "manager_id", "salary")
)
