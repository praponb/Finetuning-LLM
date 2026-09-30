from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select("id", "city", "age", "country", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
