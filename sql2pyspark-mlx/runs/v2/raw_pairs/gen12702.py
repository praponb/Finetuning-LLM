from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select("id", "city", "signup_date", "name", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "city", "signup_date", "name")
)
