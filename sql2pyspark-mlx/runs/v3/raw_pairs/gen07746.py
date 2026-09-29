from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-05-01", "2023-06-01"))
    .select("id", "country", "credit_limit", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "country", "credit_limit")
)
