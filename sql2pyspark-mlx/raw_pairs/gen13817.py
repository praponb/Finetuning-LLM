from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit") > 5700)
    .select("id", "segment", "signup_date", "city", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "segment", "signup_date", "city")
)
