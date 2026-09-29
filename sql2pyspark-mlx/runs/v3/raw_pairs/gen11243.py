from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "country", "age", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "country", "age")
)
