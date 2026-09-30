from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("country").isin(["US", "JP", "TH"]))
    .select("id", "country", "credit_limit", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .orderBy(F.col("credit_limit").desc(), F.col("id").desc())
)
