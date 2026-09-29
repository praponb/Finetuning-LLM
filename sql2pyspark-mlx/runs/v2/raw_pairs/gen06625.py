from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("name").like("C%"))
    .select("id", "city", "signup_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "city", "signup_date")
)
