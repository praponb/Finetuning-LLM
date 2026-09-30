from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select("id", "country", "credit_limit", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("id").desc(), "country")
)
