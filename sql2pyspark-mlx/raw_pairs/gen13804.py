from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")

result = (
    df_customers
    .select("id", "segment", "age", "email", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .orderBy("email")
)
