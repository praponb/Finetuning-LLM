from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("segment").isNull())
    .select("id", "segment", "credit_limit", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "segment", "credit_limit")
    .orderBy(F.col("credit_limit").desc())
)
