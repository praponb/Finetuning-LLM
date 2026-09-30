from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.length("name") >= 12)
    .select("id", "segment", "credit_limit", "name", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "segment", "credit_limit", "name")
    .orderBy("name")
)
