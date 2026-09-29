from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select("id", "segment", "age", "signup_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "segment", "age", "signup_date")
)
