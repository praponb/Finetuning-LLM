from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .select("id", "country", "credit_limit", F.rank().over(w).alias("rnk"))
    .orderBy("id")
)
