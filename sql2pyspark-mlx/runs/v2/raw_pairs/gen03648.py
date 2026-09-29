from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())

result = (
    df_customers
    .select("id", "country", "age", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "age")
)
