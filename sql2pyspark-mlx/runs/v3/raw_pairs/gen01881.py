from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("age").between(46, 62))
    .select("id", "segment", "signup_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "segment", "signup_date")
    .orderBy("signup_date")
)
