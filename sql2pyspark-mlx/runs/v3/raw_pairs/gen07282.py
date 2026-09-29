from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "city", "signup_date", "email")
    .orderBy(F.col("email").desc(), F.col("id").desc())
)
