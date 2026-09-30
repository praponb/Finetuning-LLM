from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit") > 8800)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
)
