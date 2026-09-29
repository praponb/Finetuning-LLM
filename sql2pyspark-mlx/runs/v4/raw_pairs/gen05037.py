from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "level", "hire_date")
    .orderBy("level")
)
