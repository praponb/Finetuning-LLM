from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select("id", "city", "credit_limit", "name", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "city", "credit_limit", "name")
)
