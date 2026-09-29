from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .select("id", "country", "signup_date", "city", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "country", "signup_date", "city")
    .orderBy("id")
)
