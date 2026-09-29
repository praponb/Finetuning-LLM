from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select("id", "country", "age", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "country", "age")
    .orderBy(F.col("age").desc())
)
