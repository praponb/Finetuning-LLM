from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.lower(F.trim("email")) == "liam@outlook.com")
    .select("id", "country", "credit_limit", "city", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "country", "credit_limit", "city")
)
