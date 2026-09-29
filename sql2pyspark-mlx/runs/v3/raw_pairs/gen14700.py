from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("full_name").like("%MILLE%"))
    .select("id", "level", "salary", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy("level")
)
