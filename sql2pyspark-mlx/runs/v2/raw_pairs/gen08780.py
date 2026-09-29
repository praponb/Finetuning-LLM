from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select("id", "level", "salary", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "level", "salary")
    .orderBy(F.col("salary").desc())
)
