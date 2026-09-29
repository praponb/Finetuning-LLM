from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("name").isNull())
    .select("id", "segment", "credit_limit", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy("segment")
)
