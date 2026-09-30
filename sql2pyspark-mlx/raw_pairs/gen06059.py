from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")

result = (
    df_customers
    .select("id", "country", "credit_limit", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "credit_limit")
)
