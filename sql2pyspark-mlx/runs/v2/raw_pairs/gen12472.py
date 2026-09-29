from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")

result = (
    df_customers
    .select("id", "segment", "age", "name", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "segment", "age", "name")
    .orderBy(F.col("name").desc())
)
