from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .select("id", "city", "age", "signup_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "city", "age", "signup_date")
)
