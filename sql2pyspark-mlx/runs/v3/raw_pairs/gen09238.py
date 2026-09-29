from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "Berlin"]))
    .select("id", "city", "signup_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "city", "signup_date")
    .orderBy(F.col("id").desc(), "signup_date")
)
